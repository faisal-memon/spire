//go:build test_external_database
// +build test_external_database

package sqlstore

import (
	"os"
	"testing"
)

func getDataSourceNameFromEnv(tb testing.TB, env string) string {
	dataSource := os.Getenv(env)
	if dataSource == "" {
		tb.Skipf("%s envvar is not set", env)
	}
	return dataSource
}
