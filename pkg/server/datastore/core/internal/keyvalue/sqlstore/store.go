package sqlstore

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
)

type Store interface {
	keyvalue.Store
	io.Closer
}

type SQLite3 struct {
	disabledByTag

	Path string
	Log  logr.Logger

	nowFn func() time.Time
}

type PostgreSQL struct {
	disabledByTag

	DataSourceName string
	Log            logr.Logger

	nowFn func() time.Time
}

type MySQL struct {
	disabledByTag

	DataSourceName string
	Log            logr.Logger

	nowFn func() time.Time
}

type Config interface {
	Open(ctx context.Context) (Store, error)
}

func Open[C Config](ctx context.Context, config C) (_ Store, err error) {
	return config.Open(ctx)
}

// disabledByTag is embedded in the dialect configuration structs to provide an
// implementation of Open() that returns an error indicating that the dialect
// has been disabled. The structs themselves implement their own Open
// implementation behind build tags that will be promoted over this
// implementation.
type disabledByTag struct{}

func (disabledByTag) Open(ctx context.Context) (Store, error) {
	return nil, errors.New("dialect is disabled by build tags")
}
