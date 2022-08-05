package sqlstore

import (
	"database/sql"
	"embed"
	"path"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*/*.sql
var embeddedMigrations embed.FS

func init() {
	goose.SetBaseFS(embeddedMigrations)
	goose.SetLogger(nullLogger{})
}

func migrateUp(db *sql.DB, dialect string) error {
	goose.SetDialect(dialect)
	return goose.Up(db, path.Join("migrations", dialect))
}

// stdLogger is a default logger that outputs to a stdlib's log.std logger.
type nullLogger struct{}

func (nullLogger) Fatal(v ...interface{})                 {}
func (nullLogger) Fatalf(format string, v ...interface{}) {}
func (nullLogger) Print(v ...interface{})                 {}
func (nullLogger) Println(v ...interface{})               {}
func (nullLogger) Printf(format string, v ...interface{}) {}
