package etcdstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/etcdstore/internal"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/internal/watch"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type EtcdConfig = clientv3.Config

type Config struct {
	Etcd   EtcdConfig
	Prefix string
	Now    func() time.Time
}

type Store struct {
	c       *clientv3.Client
	prefix  string
	now     func() time.Time
	watches watch.Watchers[watcher]
}

func Open(config Config) (*Store, error) {
	if config.Now == nil {
		config.Now = time.Now
	}

	c, err := clientv3.New(config.Etcd)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}
	return &Store{
		c:      c,
		prefix: config.Prefix,
		now:    config.Now,
	}, nil
}

func (s *Store) Close() error {
	s.watches.Close()
	return s.c.Close()
}

func (s *Store) Get(ctx context.Context, kind string, key string) (keyvalue.Record, error) {
	kv, err := getRecordKeyValue(ctx, s.c, s.recordKey(kind, key))
	if err != nil {
		return keyvalue.Record{}, err
	}
	return kv.Record, nil
}

func (s *Store) Create(ctx context.Context, kind string, key string, value []byte) error {
	now := s.now()
	recordKey := s.recordKey(kind, key)
	recordValue, err := encodeRecordValue(now, now, value)
	if err != nil {
		return err
	}

	resp, err := s.c.Txn(ctx).If(
		notFound(recordKey),
	).Then(
		clientv3.OpPut(recordKey, string(recordValue)),
	).Commit()

	switch {
	case err != nil:
		return err
	case len(resp.Responses) == 0:
		// Since we don't configure Else operations, responses should only
		// be non-zero if the value was successfully put. If it is empty, it
		// means that the record was found.
		return keyvalue.ErrExists
	default:
		return nil
	}
}

func (s *Store) Update(ctx context.Context, kind string, key string, value []byte, revision int64) error {
	recordKey := s.recordKey(kind, key)

	// Get the existing record and fail if the record doesn't exist or if the
	// expected revision isn't correct.
	kv, err := getRecordKeyValue(ctx, s.c, s.recordKey(kind, key))
	switch {
	case err != nil:
		return err
	case kv.Revision != revision:
		return keyvalue.ErrConflict
	}

	// Create a new value, with a new UpdatedAt timestamp.
	recordValue, err := encodeRecordValue(kv.CreatedAt, s.now(), value)
	if err != nil {
		return err
	}

	// Perform the update, but only if the mod revision holds.
	resp, err := s.c.Txn(ctx).If(
		atModRevision(recordKey, kv.ModRevision),
	).Then(
		clientv3.OpPut(recordKey, string(recordValue)),
	).Commit()

	switch {
	case err != nil:
		return err
	case len(resp.Responses) == 0:
		// Since we don't configure Else operations, responses should only
		// be non-zero if the value was successfully put. If it is empty, it
		// means that the record was not updated due to mod revision mismatch.
		// Likely reasons are that record was updated or deleted from underneath.
		return keyvalue.ErrConflict
	default:
		return nil
	}
}

func (s *Store) Replace(ctx context.Context, kind string, key string, value []byte) error {
	recordKey := s.recordKey(kind, key)

	// Get the existing record and fail if the record doesn't exist
	kv, err := getRecordKeyValue(ctx, s.c, s.recordKey(kind, key))
	if err != nil {
		return err
	}

	// Create a new value, with a new UpdatedAt timestamp.
	recordValue, err := encodeRecordValue(kv.CreatedAt, s.now(), value)
	if err != nil {
		return err
	}

	// Perform the replacement, but only if the mod revision holds.
	resp, err := s.c.Txn(ctx).If(
		atModRevision(recordKey, kv.ModRevision),
	).Then(
		clientv3.OpPut(recordKey, string(recordValue)),
	).Commit()

	switch {
	case err != nil:
		return err
	case len(resp.Responses) == 0:
		// Since we don't configure Else operations, responses should only
		// be non-zero if the value was successfully put. If it is empty, it
		// means that the record was not replaced due to revision mismatch.
		return keyvalue.ErrConflict
	default:
		return nil
	}
}

func (s *Store) Delete(ctx context.Context, kind string, key string) error {
	resp, err := s.c.Delete(ctx, s.recordKey(kind, key))
	switch {
	case err != nil:
		return err
	case resp.Deleted == 0:
		return keyvalue.ErrNotFound
	}
	return nil
}

func (s *Store) Batch(ctx context.Context, ops []keyvalue.Op) error {
	return errors.New("unimplemented")
}

func (s *Store) Watch(ctx context.Context) keyvalue.WatchChan {
	return s.watches.New(ctx, watcher{
		c:         s.c,
		prefixKey: s.prefixKey(),
	})
}

type watcher struct {
	c         *clientv3.Client
	prefixKey string
}

func (w watcher) Watch(ctx context.Context, report watch.ReportFunc) error {
	ctx = clientv3.WithRequireLeader(ctx)

	lastResp, err := w.c.Get(ctx, w.prefixKey, clientv3.WithLastRev()...)
	if err != nil {
		return err
	}

	var reportedCurrent bool
	var lastRevision int64
	if len(lastResp.Kvs) > 0 {
		lastRevision = lastResp.Kvs[0].ModRevision
	} else {
		if err := report(nil, true); err != nil {
			return err
		}
	}

	var watchChan clientv3.WatchChan
	compactRevision := int64(1)
	var changed []keyvalue.Key
	dedup := make(map[keyvalue.Key]struct{})
	for {
		if watchChan == nil {
			watchChan = w.c.Watch(ctx, w.prefixKey, clientv3.WithPrefix(), clientv3.WithRev(compactRevision))
		}

		resp := <-watchChan
		if resp.Canceled {
			if resp.CompactRevision > 0 {
				compactRevision = resp.CompactRevision
				watchChan = nil
				continue
			}
			return resp.Err()
		}

		if !reportedCurrent && resp.Header.Revision >= lastRevision {
			if err := report(nil, true); err != nil {
				return err
			}
			reportedCurrent = true
		}

		if resp.IsProgressNotify() {
			continue
		}
		for k := range dedup {
			delete(dedup, k)
		}
		changed = changed[:0]
		for _, event := range resp.Events {
			if key, ok := decodeRecordKey(string(event.Kv.Key), w.prefixKey); ok {
				if _, ok := dedup[key]; !ok {
					dedup[key] = struct{}{}
					changed = append(changed, key)
				}
			}

		}
		if len(changed) > 0 {
			if err := report(changed, false); err != nil {
				return err
			}
		}
	}
}

func (s *Store) prefixKey() string {
	k := new(strings.Builder)
	k.WriteRune('/')
	if s.prefix != "" {
		k.WriteString(s.prefix)
	}
	return k.String()
}

func (s *Store) recordKey(kind, key string) string {
	k := new(strings.Builder)
	k.WriteRune('/')
	if s.prefix != "" {
		k.WriteString(s.prefix)
		k.WriteRune('/')
	}
	k.WriteString(kind)
	k.WriteRune('/')
	k.WriteString(key)
	return k.String()
}

type recordKeyValue struct {
	keyvalue.Record
	ModRevision int64
}

func getRecordKeyValue(ctx context.Context, c clientv3.KV, recordKey string) (recordKeyValue, error) {
	resp, err := c.Get(ctx, recordKey)
	if err != nil {
		return recordKeyValue{}, err
	}
	if len(resp.Kvs) == 0 {
		return recordKeyValue{}, keyvalue.ErrNotFound
	}
	kv := resp.Kvs[0]
	return decodRecordKeyValue(kv)
}

func encodeRecordValue(createdAt, updatedAt time.Time, data []byte) ([]byte, error) {
	return proto.Marshal(&internal.Record{
		CreatedAt: createdAt.Unix(),
		UpdatedAt: updatedAt.Unix(),
		Data:      data,
	})
}

func decodRecordKeyValue(kv *mvccpb.KeyValue) (recordKeyValue, error) {
	r := new(internal.Record)
	if err := proto.Unmarshal(kv.Value, r); err != nil {
		return recordKeyValue{}, fmt.Errorf("failed to unmarshal record value: %v", err)
	}
	return recordKeyValue{
		Record: keyvalue.Record{
			Metadata: keyvalue.Metadata{
				CreatedAt: time.Unix(r.CreatedAt, 0),
				UpdatedAt: time.Unix(r.UpdatedAt, 0),
				Revision:  kv.Version,
			},
			Value: r.Data,
		},
		ModRevision: kv.ModRevision,
	}, nil
}

func decodeRecordKey(key string, prefix string) (keyvalue.Key, bool) {
	trimmed := strings.TrimPrefix(key, prefix)
	if trimmed == key {
		return keyvalue.Key{}, false
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 {
		return keyvalue.Key{}, false
	}
	return keyvalue.Key{
		Kind: parts[0],
		Key:  parts[1],
	}, true
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func atModRevision(key string, version int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", version)
}

func atVersion(key string, version int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.Version(key), "=", version)
}
