package etcdstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/etcdstore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/storetest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func Test(t *testing.T) {
	etcdConfig := clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}

	storetest.Test(t, func(_ context.Context, t *testing.T, nowFn func() time.Time) keyvalue.Store {
		clearEtcd(t, etcdConfig)
		s, err := etcdstore.Open(etcdstore.Config{
			Etcd: etcdConfig,
			Now:  nowFn,
		})
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s
	})
}

func clearEtcd(t *testing.T, config clientv3.Config) {
	c, err := clientv3.New(config)
	require.NoError(t, err)
	resp, err := c.Delete(context.Background(), "/", clientv3.WithPrefix())
	require.NoError(t, err)
	t.Logf("cleaned up %d records", resp.Deleted)
}
