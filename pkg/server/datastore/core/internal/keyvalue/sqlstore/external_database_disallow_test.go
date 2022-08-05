//go:build !test_external_database
// +build !test_external_database

package sqlstore

import "testing"

func getDataSourceNameFromEnv(tb testing.TB, env string) string {
	tb.Skip("not built with the test_external_database tag")
	return ""
}
