package core_test

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/core"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/datastoretest"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	datastoretest.Test(t, func(t *testing.T) datastore.DataStore {
		log, _ := test.NewNullLogger()
		ds, err := core.New(context.Background(), core.Config{
			Log:        log,
			DriverName: "sqlite3",
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			ds.Close()
		})
		return ds
	})
}
