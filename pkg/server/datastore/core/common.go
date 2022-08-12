package core

import (
	"errors"

	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/record"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func matchBehavior(m datastore.MatchBehavior) record.MatchBehavior {
	switch m {
	case datastore.MatchAny:
		return record.MatchAny
	case datastore.Exact:
		return record.MatchExact
	case datastore.Superset:
		return record.MatchSuperset
	case datastore.Subset:
		return record.MatchSubset
	}
	return 0
}

func compareCommonSelectors(a, b *common.Selector) int {
	switch {
	case a == nil:
		if b == nil {
			return 0
		}
		return -1
	case b == nil:
		return 1
	case a.Type < b.Type:
		return -1
	case a.Type > b.Type:
		return 1
	case a.Value < b.Value:
		return -1
	case a.Value > b.Value:
		return 1
	}
	return 0
}

func newPagination(in *datastore.Pagination, cursor string) *datastore.Pagination {
	if in != nil {
		return &datastore.Pagination{
			PageSize: in.PageSize,
			Token:    cursor,
		}
	}
	return nil
}

func getPaginationParams(p *datastore.Pagination) (string, int, error) {
	switch {
	case p == nil:
		return "", 0, nil
	case p.PageSize == 0:
		return "", 0, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	return p.Token, int(p.PageSize), nil
}

func dsErr(err error, format string, args ...interface{}) error {
	code := codes.Unknown
	switch {
	case errors.Is(err, record.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, record.ErrConflict):
		code = codes.FailedPrecondition
	case errors.Is(err, record.ErrExists):
		code = codes.AlreadyExists
	}
	args = append(args, err)
	return status.Errorf(code, format+": %v", args...)
}