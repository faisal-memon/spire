package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gofrs/uuid"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/protobuf/proto"
)

func (ds *DataStore) CountRegistrationEntries(context.Context) (int32, error) {
	return int32(ds.entries.Count()), nil
}

func (ds *DataStore) CreateRegistrationEntry(ctx context.Context, in *common.RegistrationEntry) (*common.RegistrationEntry, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return nil, dsErr(err, "failed to generate new entry ID")
	}
	in.EntryId = u.String()

	if err := ds.entries.Create(ctx, makeEntryObject(in)); err != nil {
		return nil, dsErr(err, "failed to create entry")
	}
	return in, nil
}

func (ds *DataStore) CreateOrReturnRegistrationEntry(ctx context.Context, entry *common.RegistrationEntry) (*common.RegistrationEntry, bool, error) {
	obj := makeEntryObject(entry)

	var existing *record.Record[entryObject]
	var exists bool
	ds.entries.ReadIndex(func(index *entryIndex) {
		existing, exists = index.all.Get(obj.contentKey)
	})
	if exists {
		return existing.Object.Entry, true, nil
	}

	entry, err := ds.CreateRegistrationEntry(ctx, entry)
	if err != nil {
		return nil, false, err
	}
	return entry, false, err
}

func (ds *DataStore) DeleteRegistrationEntry(ctx context.Context, entryID string) error {
	if err := ds.entries.Delete(ctx, entryID); err != nil {
		return dsErr(err, "failed to delete entry")
	}
	return nil
}

func (ds *DataStore) FetchRegistrationEntry(ctx context.Context, entryID string) (*common.RegistrationEntry, error) {
	r, err := ds.entries.Get(entryID)
	switch {
	case err == nil:
		return r.Object.Entry, nil
	case errors.Is(err, record.ErrNotFound):
		return nil, nil
	default:
		return nil, dsErr(err, "failed to fetch entry")
	}
}

func (ds *DataStore) ListRegistrationEntries(ctx context.Context, req *datastore.ListRegistrationEntriesRequest) (*datastore.ListRegistrationEntriesResponse, error) {
	records, cursor, err := ds.entries.List(req)
	if err != nil {
		return nil, err
	}
	resp := &datastore.ListRegistrationEntriesResponse{
		Pagination: newPagination(req.Pagination, cursor),
	}
	resp.Entries = make([]*common.RegistrationEntry, 0, len(records))
	for _, record := range records {
		resp.Entries = append(resp.Entries, record.Object.Entry)
	}
	return resp, nil
}

func (ds *DataStore) PruneRegistrationEntries(ctx context.Context, expiresBefore time.Time) error {
	records, _, err := ds.entries.List(&datastore.ListRegistrationEntriesRequest{
		ByExpiresBefore: expiresBefore,
	})
	if err != nil {
		return err
	}

	var errCount int
	var firstErr error
	for _, record := range records {
		if err := ds.entries.Delete(ctx, record.Object.Entry.EntryId); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			errCount++
		}
	}

	if firstErr != nil {
		return dsErr(firstErr, "failed pruning %d of %d entries: first error:", errCount, len(records))
	}
	return nil
}

func (ds *DataStore) UpdateRegistrationEntry(ctx context.Context, newEntry *common.RegistrationEntry, mask *common.RegistrationEntryMask) (*common.RegistrationEntry, error) {
	fmt.Println("UPDATE", newEntry.EntryId)
	existing, err := ds.entries.Get(newEntry.EntryId)
	if err != nil {
		return nil, dsErr(err, "failed to update entry")
	}

	updated := existing.Object

	if mask == nil {
		mask = protoutil.AllTrueCommonRegistrationEntryMask
	}

	if mask.Selectors {
		updated.Entry.Selectors = newEntry.Selectors
	}
	if mask.ParentId {
		updated.Entry.ParentId = newEntry.ParentId
	}
	if mask.SpiffeId {
		updated.Entry.SpiffeId = newEntry.SpiffeId
	}
	if mask.Ttl {
		updated.Entry.Ttl = newEntry.Ttl
	}
	if mask.FederatesWith {
		updated.Entry.FederatesWith = newEntry.FederatesWith
	}
	if mask.EntryId {
		updated.Entry.EntryId = newEntry.EntryId
	}
	if mask.Admin {
		updated.Entry.Admin = newEntry.Admin
	}
	if mask.Downstream {
		updated.Entry.Downstream = newEntry.Downstream
	}
	if mask.EntryExpiry {
		updated.Entry.EntryExpiry = newEntry.EntryExpiry
	}
	if mask.DnsNames {
		updated.Entry.DnsNames = newEntry.DnsNames
	}
	if mask.StoreSvid {
		updated.Entry.StoreSvid = newEntry.StoreSvid
	}

	if err := ds.entries.Update(ctx, updated, existing.Metadata.Revision); err != nil {
		return nil, dsErr(err, "failed to update entry")
	}
	return updated.Entry, nil
}

type entryObject struct {
	contentKey string
	Entry      *common.RegistrationEntry
}

func makeEntryObject(entry *common.RegistrationEntry) entryObject {
	return entryObject{
		contentKey: entryContentKey(entry),
		Entry:      entry,
	}
}

func (o entryObject) Key() string {
	return o.contentKey
}

func entryContentKey(entry *common.RegistrationEntry) string {
	h := sha256.New()
	io.WriteString(h, entry.SpiffeId)
	io.WriteString(h, entry.ParentId)
	return base64.URLEncoding.EncodeToString(h.Sum(nil))
}

type entryCodec struct{}

func (entryCodec) Marshal(in *entryObject) (string, []byte, error) {
	out, err := proto.Marshal(in.Entry)
	if err != nil {
		return "", nil, err
	}
	return in.contentKey, out, nil
}

func (entryCodec) Unmarshal(in []byte, out *entryObject) error {
	entry := new(common.RegistrationEntry)
	if err := proto.Unmarshal(in, entry); err != nil {
		return err
	}
	out.Entry = entry
	out.contentKey = entryContentKey(out.Entry)
	return nil
}

type entryIndex struct {
	all           record.Set[entryObject]
	parentID      record.UnaryIndex[entryObject, string]
	spiffeID      record.UnaryIndex[entryObject, string]
	selectors     record.MultiIndexCmp[entryObject, *common.Selector, selectorCmp]
	federatesWith record.MultiIndex[entryObject, string]
	expiresAt     record.UnaryIndex[entryObject, int64]
}

func (c *entryIndex) Count() int {
	return c.all.Count()
}

func (c *entryIndex) Get(key string) (*record.Record[entryObject], bool) {
	return c.all.Get(key)
}

func (c *entryIndex) Put(r *record.Record[entryObject]) error {
	r.Object.Entry.RevisionNumber = r.Metadata.Revision

	c.all.Set(r)
	c.parentID.Set(r, r.Object.Entry.ParentId)
	c.spiffeID.Set(r, r.Object.Entry.SpiffeId)
	c.selectors.Set(r, r.Object.Entry.Selectors)
	c.federatesWith.Set(r, r.Object.Entry.FederatesWith)
	c.expiresAt.Set(r, r.Object.Entry.EntryExpiry)
	return nil
}

func (c *entryIndex) Delete(key string) {
	c.all.Delete(key)
	c.parentID.Delete(key)
	c.spiffeID.Delete(key)
	c.selectors.Delete(key)
	c.federatesWith.Delete(key)
	c.expiresAt.Delete(key)
}

func (c *entryIndex) List(req *datastore.ListRegistrationEntriesRequest) (record.Iterator[entryObject], error) {
	cursor, limit, err := getPaginationParams(req.Pagination)
	if err != nil {
		return nil, err
	}

	var filters []record.Iterator[entryObject]
	if req.ByParentID != "" {
		filters = append(filters, c.parentID.EqualTo(cursor, req.ByParentID))
	}
	if req.BySpiffeID != "" {
		filters = append(filters, c.spiffeID.EqualTo(cursor, req.BySpiffeID))
	}
	if req.BySelectors != nil {
		filters = append(filters, c.selectors.Matching(cursor, req.BySelectors.Selectors, matchBehavior(req.BySelectors.Match)))
	}
	if req.ByFederatesWith != nil {
		filters = append(filters, c.federatesWith.Matching(cursor, req.ByFederatesWith.TrustDomains, matchBehavior(req.ByFederatesWith.Match)))
	}
	if !req.ByExpiresBefore.IsZero() {
		filters = append(filters, c.expiresAt.LessThan(cursor, req.ByExpiresBefore.Unix()))
	}

	var iter record.Iterator[entryObject]
	if len(filters) > 0 {
		iter = record.And(filters)
	} else {
		iter = c.all.Iterate(cursor)
	}

	if limit > 0 {
		iter = record.Limit(iter, limit)
	}
	return iter, nil
}
