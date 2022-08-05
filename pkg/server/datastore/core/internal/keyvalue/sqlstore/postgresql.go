//go:build !no_postgres
// +build !no_postgres

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/internal/watch"
)

func (c PostgreSQL) Open(ctx context.Context) (_ Store, err error) {
	if c.nowFn == nil {
		c.nowFn = time.Now
	}
	s := &postgreSQLStore{log: c.Log, nowFn: c.nowFn}

	// Ensure the store is cleaned up on failure.
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	s.db, err = sql.Open("postgres", c.DataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	if err := s.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping store: %w", err)
	}

	if err := migrateUp(s.db, "postgresql"); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	// Set up our prepared statements
	s.stmtRevision, err = s.db.PrepareContext(ctx, `SELECT revision FROM records WHERE kind=$1 AND key=$2`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare revision statement: %w", err)
	}

	s.stmtGet, err = s.db.PrepareContext(ctx, `SELECT created_at, updated_at, revision, data FROM records WHERE kind=$1 AND key=$2`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get statement: %w", err)
	}

	s.stmtCreate, err = s.db.PrepareContext(ctx, `
		INSERT INTO records (kind, key, data, created_at, updated_at, revision) VALUES ($1, $2, $3, $4, $5, 0)`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare create statement: %w", err)
	}

	s.stmtReplace, err = s.db.PrepareContext(ctx, `
		UPDATE records SET data=$1, updated_at=$2, revision=revision+1 WHERE kind=$3 AND key=$4`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare replace statement: %w", err)
	}

	s.stmtDelete, err = s.db.PrepareContext(ctx, `DELETE FROM records WHERE kind=$1 AND key=$2`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	s.stmtListRecords, err = s.db.PrepareContext(ctx, `SELECT id, kind, key FROM records WHERE id > $1 ORDER BY id ASC LIMIT $2`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list records statement: %w", err)
	}

	s.stmtListChanges, err = s.db.PrepareContext(ctx, `SELECT id, kind, key FROM changes WHERE id > $1 ORDER BY id ASC LIMIT $2`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list changes statement: %w", err)
	}

	s.stmtDropChanges, err = s.db.PrepareContext(ctx, `DELETE FROM changes WHERE timestamp < $1`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare drop changes statement: %w", err)
	}

	return s, nil

}

type postgreSQLStore struct {
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

func (s *postgreSQLStore) Close() error {
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

func (s *postgreSQLStore) Get(ctx context.Context, kind, key string) (keyvalue.Record, error) {
	row := s.stmtGet.QueryRowContext(ctx, kind, key)

	var r keyvalue.Record
	err := row.Scan(&r.CreatedAt, &r.UpdatedAt, &r.Revision, &r.Value)
	switch {
	case err == nil:
		return r, nil
	case errors.Is(err, sql.ErrNoRows):
		return keyvalue.Record{}, fmt.Errorf("failed to get %q/%q: %w", kind, key, keyvalue.ErrNotFound)
	default:
		return keyvalue.Record{}, fmt.Errorf("failed to get %q/%q: %w", kind, key, err)
	}
}

func (s *postgreSQLStore) Create(ctx context.Context, kind string, key string, data []byte) error {
	now := s.now()
	_, err := s.stmtCreate.ExecContext(ctx, kind, key, data, now, now)
	if err != nil {
		err = convertPostgreSQLError(err)
		return fmt.Errorf("failed to create %q/%q: %w", kind, key, err)
	}
	return nil
}

func (s *postgreSQLStore) Replace(ctx context.Context, kind string, key string, data []byte) error {
	now := s.now()
	result, err := s.stmtReplace.ExecContext(ctx, data, now, kind, key)
	if err != nil {
		err = convertPostgreSQLError(err)
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

func (s *postgreSQLStore) Update(ctx context.Context, kind string, key string, data []byte, revision int64) error {
	return withTx(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		row := tx.Stmt(s.stmtRevision).QueryRowContext(ctx, kind, key)
		var actualRevision int64
		if err := row.Scan(&actualRevision); err != nil {
			err = convertPostgreSQLError(err)
			return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
		}

		if actualRevision != revision {
			return fmt.Errorf("failed to update %q/%q: %w: expected revision %d but got %d", kind, key, keyvalue.ErrConflict, revision, actualRevision)
		}

		now := s.now()
		if _, err := tx.Stmt(s.stmtReplace).ExecContext(ctx, data, now, kind, key); err != nil {
			err = convertPostgreSQLError(err)
			return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
		}
		return nil
	})
}

func (s *postgreSQLStore) Delete(ctx context.Context, kind string, key string) error {
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

func (s *postgreSQLStore) Batch(ctx context.Context, ops []keyvalue.Op) error {
	return errors.New("unimplemented")
}

func (s *postgreSQLStore) Watch(ctx context.Context) keyvalue.WatchChan {
	return s.watchers.New(ctx, &watcher{
		log:         s.log,
		listRecords: s.stmtListRecords,
		listChanges: s.stmtListChanges,
	})
}

func (s *postgreSQLStore) now() time.Time {
	return s.nowFn().UTC()
}

func convertPostgreSQLError(err error) error {
	var e *pq.Error
	if ok := errors.As(err, &e); ok {
		switch e.Code.Class() {
		// "23xxx" is the constraint violation class for PostgreSQL
		case "23":
			return keyvalue.ErrExists
		}
	}
	return err
}
