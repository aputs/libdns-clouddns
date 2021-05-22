// Package clouddns implements a DNS record management client compatible
// with the libdns interfaces for google clouddns.
package clouddns

import (
	"context"
	"fmt"

	"github.com/libdns/libdns"
	dns "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Provider facilitates DNS record manipulation with clouddns
type Provider struct {
	Project     string `json:"project"`
	JsonKeyFile string `json:"json_key_file,omitempty"`
	service     *dns.Service
}

func (p *Provider) NewSession(ctx context.Context) error {
	if p.JsonKeyFile != "" {
		service, err := dns.NewService(ctx, option.WithCredentialsFile(p.JsonKeyFile))
		if err != nil {
			return err
		}
		p.service = service
	} else {
		service, err := dns.NewService(ctx)
		if err == nil {
			return err
		}
		p.service = service
	}
	return nil
}

func (p *Provider) getZone(zone string) (*dns.ManagedZone, error) {
	managedZonesService := dns.NewManagedZonesService(p.service)
	managedZones, err := managedZonesService.List(p.Project).DnsName(zone).MaxResults(1).Do()
	if err != nil {
		return nil, err
	}
	if len(managedZones.ManagedZones) == 0 {
		return nil, fmt.Errorf("No ManagedZone found!")
	}
	return managedZones.ManagedZones[0], nil
}

func (p *Provider) createRecords(managedZone *dns.ManagedZone, records []libdns.Record) ([]libdns.Record, error) {
	var createdRecords []libdns.Record

	changesService := dns.NewChangesService(p.service)
	change := dns.Change{
		Additions: []*dns.ResourceRecordSet{},
	}

	for _, record := range records {
		newRecord := dns.ResourceRecordSet{
			Type:    record.Type,
			Name:    libdns.AbsoluteName(record.Name, managedZone.DnsName),
			Rrdatas: []string{record.Value},
			Ttl:     int64(record.TTL),
		}
		change.Additions = append(change.Additions, &newRecord)
		createdRecords = append(createdRecords, record)
	}

	if _, err := changesService.Create(p.Project, managedZone.Name, &change).Do(); err != nil {
		return nil, err
	}

	return createdRecords, nil
}

// GetRecords lists all the records in the zone.
func (p *Provider) GetRecords(ctx context.Context, zone string) ([]libdns.Record, error) {
	return nil, fmt.Errorf("TODO: not implemented1")
}

func (p *Provider) deleteRecords(managedZone *dns.ManagedZone, records []libdns.Record) ([]libdns.Record, error) {
	var deletedRecords []libdns.Record

	changesService := dns.NewChangesService(p.service)
	change := dns.Change{
		Deletions: []*dns.ResourceRecordSet{},
	}

	for _, record := range records {
		deleteRecord := dns.ResourceRecordSet{
			Type:    record.Type,
			Name:    libdns.AbsoluteName(record.Name, managedZone.DnsName),
			Rrdatas: []string{record.Value},
			Ttl:     int64(record.TTL),
		}
		change.Deletions = append(change.Deletions, &deleteRecord)
		deletedRecords = append(deletedRecords, record)
	}

	if _, err := changesService.Create(p.Project, managedZone.Name, &change).Do(); err != nil {
		return nil, err
	}

	return deletedRecords, nil
}

// AppendRecords adds records to the zone. It returns the records that were added.
func (p *Provider) AppendRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	managedZone, err := p.getZone(zone)
	if err != nil {
		return nil, err
	}

	createdRecords, err := p.createRecords(managedZone, records)
	if err != nil && err.(*googleapi.Error).Code == 409 {
		// records exists remove first then call again
		if _, err = p.deleteRecords(managedZone, records); err != nil {
			return nil, err
		}
		createdRecords, err = p.createRecords(managedZone, records)
	}

	if err != nil {
		return nil, err
	}

	return createdRecords, nil
}

// SetRecords sets the records in the zone, either by updating existing records or creating new ones.
// It returns the updated records.
func (p *Provider) SetRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	return nil, fmt.Errorf("TODO: SetRecords not implemented")
}

// DeleteRecords deletes the records from the zone. It returns the records that were deleted.
func (p *Provider) DeleteRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	managedZone, err := p.getZone(zone)
	if err != nil {
		return nil, err
	}

	deletedRecords, err := p.deleteRecords(managedZone, records)
	if err != nil {
		return nil, err
	}

	return deletedRecords, nil
}

// Interface guards
var (
	_ libdns.RecordGetter   = (*Provider)(nil)
	_ libdns.RecordAppender = (*Provider)(nil)
	_ libdns.RecordSetter   = (*Provider)(nil)
	_ libdns.RecordDeleter  = (*Provider)(nil)
)
