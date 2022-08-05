package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/internal/watch"
)

const (
	defaultPollInterval = time.Second
	listLimit           = 256
)

type watcher struct {
	log          logr.Logger
	pollInterval time.Duration
	listRecords  *sql.Stmt
	listChanges  *sql.Stmt
	cursor       int64
}

func (w *watcher) Watch(ctx context.Context, report watch.ReportFunc) error {
	pollInterval := w.pollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	if err := w.loadAll(ctx, report); err != nil {
		return err
	}

	if err := report(nil, true); err != nil {
		return err
	}

	w.cursor = -1
	timer := time.NewTimer(w.pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			w.loadChanges(ctx, report)
			timer.Reset(w.pollInterval)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *watcher) loadAll(ctx context.Context, report watch.ReportFunc) error {
	var cursor int64 = -1
	for {
		var keys []keyvalue.Key
		var err error
		keys, cursor, err = listKeys(ctx, w.listRecords, cursor, listLimit)
		if err != nil {
			return fmt.Errorf("failed to load all: %w", err)
		}
		if len(keys) == 0 {
			return nil
		}
		if err := report(keys, true); err != nil {
			return err
		}
	}
}

func (w *watcher) loadChanges(ctx context.Context, report watch.ReportFunc) error {
	for {
		keys, cursor, err := listKeys(ctx, w.listChanges, w.cursor, listLimit)
		if err != nil {
			return fmt.Errorf("failed to load changes: %w", err)
		}
		w.cursor = cursor

		if len(keys) == 0 {
			return nil
		}

		if err := report(keys, false); err != nil {
			return err
		}
	}
}
