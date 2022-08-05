//go:build !no_postgres
// +build !no_postgres

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/storetest"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQL(t *testing.T) {
	// The following envvar must be set to run tests against postgres.
	// For example:
	// "postgres://postgres:password@localhost:9999/postgres?sslmode=disable"
	const dataSourceNameEnv = "SQLSTORE_POSTGRES_TEST_DSN"

	dataSourceName := getDataSourceNameFromEnv(t, dataSourceNameEnv)

	storetest.Test(t, func(ctx context.Context, t *testing.T, nowFn func() time.Time) keyvalue.Store {
		s, err := (PostgreSQL{
			DataSourceName: dataSourceName,
			nowFn:          nowFn,
		}).Open(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s
	})
}
