//go:build !no_mysql
// +build !no_mysql

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/storetest"
	"github.com/stretchr/testify/require"
)

func TestMySQL(t *testing.T) {
	// The following envvar must be set to run tests against MySQL.
	// For example:
	// "spire:test@tcp(localhost:9999)/spire"
	const dataSourceNameEnv = "SQLSTORE_MYSQL_TEST_DSN"

	dataSourceName := getDataSourceNameFromEnv(t, dataSourceNameEnv)

	storetest.Test(t, func(ctx context.Context, t *testing.T, nowFn func() time.Time) keyvalue.Store {
		s, err := (MySQL{
			DataSourceName: dataSourceName,
			nowFn:          nowFn,
		}).Open(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s
	})
}
