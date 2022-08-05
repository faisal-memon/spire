package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
)

func listKeys(ctx context.Context, stmt *sql.Stmt, cursor int64, limit int) ([]keyvalue.Key, int64, error) {
	rows, err := stmt.QueryContext(ctx, cursor, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var ids []keyvalue.Key
	for rows.Next() {
		var id int64
		var kind string
		var key string
		if err := rows.Scan(&id, &kind, &key); err != nil {
			return nil, 0, fmt.Errorf("failed to scan listing: %w", err)
		}
		cursor = id
		ids = append(ids, keyvalue.Key{
			Kind: kind,
			Key:  key,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to enumerate listing: %w", err)
	}

	if err := rows.Close(); err != nil {
		return nil, 0, fmt.Errorf("failed to close listing results: %w", err)
	}

	return ids, cursor, nil
}

func withTx(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to open transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			if err := tx.Commit(); err != nil {
				err = fmt.Errorf("failed to commit transaction: %w", err)
			}
		}
	}()
	return fn(ctx, tx)
}

func convertErr(err error) error {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return keyvalue.ErrNotFound
	default:
		return err
	}
}
