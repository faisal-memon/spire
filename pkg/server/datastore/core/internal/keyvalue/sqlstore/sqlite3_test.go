//go:build !no_sqlite3
// +build !no_sqlite3

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/storetest"
	"github.com/stretchr/testify/require"
)

func TestSQLite3(t *testing.T) {
	storetest.Test(t, func(ctx context.Context, t *testing.T, nowFn func() time.Time) keyvalue.Store {
		s, err := (SQLite3{nowFn: nowFn}).Open(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s
	})
}
