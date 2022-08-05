//go:build !no_mysql
// +build !no_mysql

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/go-logr/logr"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/internal/watch"
)

func (c MySQL) Open(ctx context.Context) (_ Store, err error) {
	if c.nowFn == nil {
		c.nowFn = time.Now
	}

	// Parse the DSN to ensure parseTime and multiStatements are set
	dsn, err := mysql.ParseDSN(c.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %v", err)
	}
	dsn.ParseTime = true
	dsn.MultiStatements = true

	s := &mySQLStore{log: c.Log, nowFn: c.nowFn}
	// Ensure the store is cleaned up on failure.
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	s.db, err = sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	if err := s.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping store: %w", err)
	}

	if err := migrateUp(s.db, "mysql"); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	// Set up our prepared statements
	s.stmtRevision, err = s.db.PrepareContext(ctx, "SELECT revision FROM records WHERE kind=? AND `key`=?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare revision statement: %w", err)
	}

	s.stmtGet, err = s.db.PrepareContext(ctx, "SELECT created_at, updated_at, revision, tiny_data, small_data, medium_data, large_data FROM records WHERE kind=? AND `key`=?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get statement: %w", err)
	}

	s.stmtCreate, err = s.db.PrepareContext(ctx, "INSERT INTO records (kind, `key`, tiny_data, small_data, medium_data, large_data, created_at, updated_at, revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare create statement: %w", err)
	}

	s.stmtReplace, err = s.db.PrepareContext(ctx, "UPDATE records SET tiny_data=?, small_data=?, medium_data=?, large_data=?, updated_at=?, revision=revision+1 WHERE kind=? AND `key`=?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare replace statement: %w", err)
	}

	s.stmtDelete, err = s.db.PrepareContext(ctx, "DELETE FROM records WHERE kind=? AND `key`=?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	s.stmtListRecords, err = s.db.PrepareContext(ctx, "SELECT id, kind, `key` FROM records WHERE id > ? ORDER BY id ASC LIMIT ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list records statement: %w", err)
	}

	s.stmtListChanges, err = s.db.PrepareContext(ctx, "SELECT id, kind, `key` FROM changes WHERE id > ? ORDER BY id ASC LIMIT ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list changes statement: %w", err)
	}

	s.stmtDropChanges, err = s.db.PrepareContext(ctx, "DELETE FROM changes WHERE timestamp < ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare drop changes statement: %w", err)
	}

	return s, nil
}

type mySQLStore struct {
	log   logr.Logger
	nowFn func() time.Time

	db *sql.DB

	watchers watch.Watchers[*watcher]

	// prepared statements
	stmtRevision    *sql.Stmt
	stmtGet         *sql.Stmt
	stmtCreate      *sql.Stmt
	stmtReplace     *sql.Stmt
	stmtDelete      *sql.Stmt
	stmtListRecords *sql.Stmt
	stmtListChanges *sql.Stmt
	stmtDropChanges *sql.Stmt
}

func (s *mySQLStore) Close() error {
	var lastErr error
	doClose := func(c io.Closer) {
		if err := c.Close(); err != nil {
			lastErr = err
		}
	}

	if s.stmtDropChanges != nil {
		doClose(s.stmtDropChanges)
	}
	if s.stmtListChanges != nil {
		doClose(s.stmtListChanges)
	}
	if s.stmtListRecords != nil {
		doClose(s.stmtListRecords)
	}
	if s.stmtDelete != nil {
		doClose(s.stmtDelete)
	}
	if s.stmtReplace != nil {
		doClose(s.stmtReplace)
	}
	if s.stmtCreate != nil {
		doClose(s.stmtCreate)
	}
	if s.stmtGet != nil {
		doClose(s.stmtGet)
	}
	if s.stmtRevision != nil {
		doClose(s.stmtRevision)
	}
	if s.db != nil {
		doClose(s.db)
	}
	return lastErr
}

func (s *mySQLStore) Get(ctx context.Context, kind, key string) (keyvalue.Record, error) {
	row := s.stmtGet.QueryRowContext(ctx, kind, key)

	var r keyvalue.Record
	var tiny []byte
	var small []byte
	var medium []byte
	var large []byte
	err := row.Scan(&r.CreatedAt, &r.UpdatedAt, &r.Revision, &tiny, &small, &medium, &large)
	switch {
	case err == nil:
		r.Value = valueFromColumns(tiny, small, medium, large)
		return r, nil
	case errors.Is(err, sql.ErrNoRows):
		return keyvalue.Record{}, fmt.Errorf("failed to get %q/%q: %w", kind, key, keyvalue.ErrNotFound)
	default:
		return keyvalue.Record{}, fmt.Errorf("failed to get %q/%q: %w", kind, key, err)
	}
}

func (s *mySQLStore) Create(ctx context.Context, kind string, key string, data []byte) error {
	now := s.now()

	tiny, small, medium, large := valueToColumns(data)
	_, err := s.stmtCreate.ExecContext(ctx, kind, key, tiny, small, medium, large, now, now)
	if err != nil {
		err = convertMySQLError(err)
		return fmt.Errorf("failed to create %q/%q: %w", kind, key, err)
	}
	return nil
}

func (s *mySQLStore) Replace(ctx context.Context, kind string, key string, data []byte) error {
	now := s.now()
	tiny, small, medium, large := valueToColumns(data)
	result, err := s.stmtReplace.ExecContext(ctx, tiny, small, medium, large, now, kind, key)
	if err != nil {
		err = convertMySQLError(err)
		return fmt.Errorf("failed to replace %q/%q: %w", kind, key, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to replace %q/%q: %w", kind, key, err)
	}
	if rowsAffected == 0 {
		err = keyvalue.ErrNotFound
		return fmt.Errorf("failed to replace %q/%q: %w", kind, key, err)
	}

	return nil
}

func (s *mySQLStore) Update(ctx context.Context, kind string, key string, data []byte, revision int64) error {
	return withTx(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		row := tx.Stmt(s.stmtRevision).QueryRowContext(ctx, kind, key)
		var actualRevision int64
		if err := row.Scan(&actualRevision); err != nil {
			err = convertMySQLError(err)
			return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
		}

		if actualRevision != revision {
			return fmt.Errorf("failed to update %q/%q: %w: expected revision %d but got %d", kind, key, keyvalue.ErrConflict, revision, actualRevision)
		}

		now := s.now()
		tiny, small, medium, large := valueToColumns(data)
		if _, err := tx.Stmt(s.stmtReplace).ExecContext(ctx, tiny, small, medium, large, now, kind, key); err != nil {
			err = convertMySQLError(err)
			return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
		}
		return nil
	})
}

func (s *mySQLStore) Delete(ctx context.Context, kind string, key string) error {
	result, err := s.stmtDelete.ExecContext(ctx, kind, key)
	if err != nil {
		return fmt.Errorf("failed to delete %q/%q: %w", kind, key, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to determined affected rows after delete %q/%q: %w", kind, key, err)
	}
	if affected == 0 {
		return keyvalue.ErrNotFound
	}
	return nil
}

func (s *mySQLStore) Batch(ctx context.Context, ops []keyvalue.Op) error {
	return errors.New("unimplemented")
}

func (s *mySQLStore) Watch(ctx context.Context) keyvalue.WatchChan {
	return s.watchers.New(ctx, &watcher{
		log:         s.log,
		listRecords: s.stmtListRecords,
		listChanges: s.stmtListChanges,
	})
}

func (s *mySQLStore) now() time.Time {
	return s.nowFn().UTC()
}

func valueToColumns(data []byte) (tiny, small, medium, large []byte) {
	switch {
	case len(data) <= 255:
		return data, nil, nil, nil
	case len(data) <= 65_535:
		return nil, data, nil, nil
	case len(data) <= 16_177_215:
		return nil, nil, data, nil
	default:
		return nil, nil, nil, data
	}
}

func valueFromColumns(tiny, small, medium, large []byte) []byte {
	switch {
	case len(tiny) > 0:
		return tiny
	case len(small) > 0:
		return small
	case len(medium) > 0:
		return medium
	default:
		return large
	}
}

func convertMySQLError(err error) error {
	var e *mysql.MySQLError
	if errors.As(err, &e) {
		switch e.Number {
		case 1062:
			// ER_DUP_ENTRY
			return keyvalue.ErrExists
		}
	}
	return err
}
