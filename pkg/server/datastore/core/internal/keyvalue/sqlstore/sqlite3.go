//go:build !no_mysql
// +build !no_mysql

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/internal/watch"
	//_ "modernc.org/sqlite"
)

var (
	nextID uint64
)

func (s SQLite3) Open(ctx context.Context) (Store, error) {
	var dsnURI string
	if s.Path == "" {
		dsnURI = newMemoryURI()
	} else {
		q := make(url.Values)
		q.Set("_journal_mode", "WAL")
		dsnURI = (&url.URL{
			Scheme:   "file",
			Opaque:   s.Path,
			RawQuery: q.Encode(),
		}).String()
	}
	return openSQLite3(ctx, dsnURI, s.Log, s.nowFn)
}

type sqlite3Store struct {
	log   logr.Logger
	nowFn func() time.Time

	db *sql.DB

	watchers watch.Watchers[*watcher]

	// writeMtx is used to serialize writes; a requirement for sqlite3
	writeMtx sync.Mutex

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

func newMemoryURI() string {
	id := atomic.AddUint64(&nextID, 1)
	return fmt.Sprintf("file:store%d?mode=memory&cache=shared", id)
}

func openSQLite3(ctx context.Context, dsnURI string, log logr.Logger, nowFn func() time.Time) (_ Store, err error) {
	if nowFn == nil {
		nowFn = time.Now
	}

	s := &sqlite3Store{log: log, nowFn: nowFn}

	// Ensure the store is cleaned up on failure.
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	s.db, err = sql.Open("sqlite3", dsnURI)
	if err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	if err := s.db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping store: %w", err)
	}

	if err := migrateUp(s.db, "sqlite3"); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	// Set up our prepared statements
	s.stmtRevision, err = s.db.PrepareContext(ctx, `SELECT revision FROM records WHERE kind=? AND key=?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get statement: %w", err)
	}

	s.stmtGet, err = s.db.PrepareContext(ctx, `SELECT created_at, updated_at, revision, data FROM records WHERE kind=? AND key=?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare get statement: %w", err)
	}

	s.stmtCreate, err = s.db.PrepareContext(ctx, `
		INSERT INTO records (kind, key, data, created_at, updated_at, revision) VALUES (?, ?, ?, ?, ?, 0)
			`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare create statement: %w", err)
	}

	s.stmtReplace, err = s.db.PrepareContext(ctx, `
		UPDATE records SET data=?, updated_at=?, revision=revision+1 WHERE kind=? AND key=?
			`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare replace statement: %w", err)
	}

	s.stmtDelete, err = s.db.PrepareContext(ctx, `DELETE FROM records WHERE kind=? AND key=?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	s.stmtListRecords, err = s.db.PrepareContext(ctx, `SELECT id, kind, key FROM records WHERE id > ? ORDER BY id ASC LIMIT ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list records statement: %w", err)
	}

	s.stmtListChanges, err = s.db.PrepareContext(ctx, `SELECT id, kind, key FROM changes WHERE id > ? ORDER BY id ASC LIMIT ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare list changes statement: %w", err)
	}

	s.stmtDropChanges, err = s.db.PrepareContext(ctx, `DELETE FROM changes WHERE timestamp < ?`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare drop changes statement: %w", err)
	}

	return s, nil
}

func (s *sqlite3Store) Close() error {
	s.watchers.Close()

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

func (s *sqlite3Store) Get(ctx context.Context, kind, key string) (keyvalue.Record, error) {
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

func (s *sqlite3Store) Create(ctx context.Context, kind string, key string, data []byte) error {
	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()
	return s.createOp(ctx, nil, kind, key, data, s.now())
}

func (s *sqlite3Store) Replace(ctx context.Context, kind string, key string, data []byte) error {
	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()
	return s.replaceOp(ctx, nil, kind, key, data, s.now())
}

func (s *sqlite3Store) Update(ctx context.Context, kind string, key string, data []byte, revision int64) error {
	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()
	return withTx(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		return s.updateOp(ctx, tx, kind, key, data, revision, s.now())
	})
}

func (s *sqlite3Store) Delete(ctx context.Context, kind string, key string) error {
	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()
	return s.deleteOp(ctx, nil, kind, key)
}

func (s *sqlite3Store) Batch(ctx context.Context, ops []keyvalue.Op) error {
	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()
	now := s.now()
	return withTx(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		for _, op := range ops {
			var err error
			switch op.Type {
			case keyvalue.CreateOp:
				err = s.createOp(ctx, tx, op.Kind, op.Key, op.Value, now)
			case keyvalue.UpdateOp:
				err = s.updateOp(ctx, tx, op.Kind, op.Key, op.Value, op.Revision, now)
			case keyvalue.ReplaceOp:
				err = s.replaceOp(ctx, tx, op.Kind, op.Key, op.Value, now)
			case keyvalue.DeleteOp:
				err = s.deleteOp(ctx, tx, op.Kind, op.Key)
			default:
				err = fmt.Errorf("unsupported batch operation type: %v", op.Type)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *sqlite3Store) createOp(ctx context.Context, tx *sql.Tx, kind string, key string, data []byte, now time.Time) error {
	_, err := wrapStmt(ctx, tx, s.stmtCreate).ExecContext(ctx, kind, key, data, now, now)
	if err != nil {
		err = convertSQLite3Error(err)
		return fmt.Errorf("failed to create %q/%q: %w", kind, key, err)
	}
	return nil
}

func (s *sqlite3Store) replaceOp(ctx context.Context, tx *sql.Tx, kind string, key string, data []byte, now time.Time) error {
	result, err := wrapStmt(ctx, tx, s.stmtReplace).ExecContext(ctx, data, now, kind, key)
	if err != nil {
		err = convertSQLite3Error(err)
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

func (s *sqlite3Store) updateOp(ctx context.Context, tx *sql.Tx, kind string, key string, data []byte, revision int64, now time.Time) error {
	row := wrapStmt(ctx, tx, s.stmtRevision).QueryRowContext(ctx, kind, key)
	var actualRevision int64
	if err := row.Scan(&actualRevision); err != nil {
		err = convertSQLite3Error(err)
		return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
	}

	if actualRevision != revision {
		return fmt.Errorf("failed to update %q/%q: %w: expected revision %d but got %d", kind, key, keyvalue.ErrConflict, revision, actualRevision)
	}

	if _, err := wrapStmt(ctx, tx, s.stmtReplace).ExecContext(ctx, data, now, kind, key); err != nil {
		err = convertSQLite3Error(err)
		return fmt.Errorf("failed to update %q/%q: %w", kind, key, err)
	}
	return nil
}

func (s *sqlite3Store) deleteOp(ctx context.Context, tx *sql.Tx, kind string, key string) error {
	result, err := wrapStmt(ctx, tx, s.stmtDelete).ExecContext(ctx, kind, key)
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

func wrapStmt(ctx context.Context, tx *sql.Tx, stmt *sql.Stmt) *sql.Stmt {
	if tx == nil {
		return stmt
	}
	return tx.StmtContext(ctx, stmt)
}

type Op struct {
	Kind  string
	Key   string
	Value []byte
	Type  OpType
}

type OpType int

const (
	CreateOp OpType = iota
	UpdateOp
	ReplaceOp
	DeleteOp
)

func (s *sqlite3Store) Watch(ctx context.Context) keyvalue.WatchChan {
	return s.watchers.New(ctx, &watcher{
		log:         s.log,
		listRecords: s.stmtListRecords,
		listChanges: s.stmtListChanges,
	})
}

func (s *sqlite3Store) now() time.Time {
	return s.nowFn().UTC()
}

func convertSQLite3Error(err error) error {
	var e sqlite3.Error
	if errors.As(err, &e) {
		switch e.Code {
		case sqlite3.ErrConstraint:
			return keyvalue.ErrExists
		}
	}
	return err
}
