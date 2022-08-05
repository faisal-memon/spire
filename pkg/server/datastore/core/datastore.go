package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/etcdstore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/keyvalue/sqlstore"
	"github.com/spiffe/spire/pkg/server/datastore/core/internal/record"
)

type Config struct {
	Log            logrus.FieldLogger
	DriverName     string
	DataSourceName string
}

type DataStore struct {
	datastore.DataStore

	log                     logrus.FieldLogger
	store                   keyvalue.Store
	agents                  *record.Cache[agentCodec, *agentIndex, agentObject, *datastore.ListAttestedNodesRequest]
	bundles                 *record.Cache[bundleCodec, *bundleIndex, bundleObject, *datastore.ListBundlesRequest]
	entries                 *record.Cache[entryCodec, *entryIndex, entryObject, *datastore.ListRegistrationEntriesRequest]
	joinTokens              *record.Cache[joinTokenCodec, *joinTokenIndex, joinTokenObject, *listJoinTokens]
	federationRelationships *record.Cache[federationRelationshipCodec, *federationRelationshipIndex, federationRelationshipObject, *datastore.ListFederationRelationshipsRequest]
	watch                   io.Closer
}

func New(ctx context.Context, config Config) (_ *DataStore, err error) {
	var store keyvalue.Store
	switch strings.ToLower(config.DriverName) {
	case "sqlite", "sqlite3":
		store, err = sqlstore.Open(ctx, sqlstore.SQLite3{
			// TODO: logging
			//Log:  logr.Logger{}, //logrLogger{log: config.Log},
			Path: config.DataSourceName,
		})
	case "postgres", "postgresql":
		store, err = sqlstore.Open(ctx, sqlstore.PostgreSQL{DataSourceName: config.DataSourceName})
	case "etcd":
		store, err = etcdstore.Open(etcdstore.Config{
			Etcd: etcdstore.EtcdConfig{
				Endpoints: []string{"127.0.0.1:2379"},
				Context:   ctx,
			},
		})
	default:
		err = errors.New("unsupported driver")
	}
	if err != nil {
		return nil, fmt.Errorf("unable to open store (driver=%q): %w", config.DriverName, err)
	}

	ds := &DataStore{
		log:                     config.Log,
		store:                   store,
		agents:                  record.NewCache[agentCodec, *agentIndex, agentObject, *datastore.ListAttestedNodesRequest](store, "agent", new(agentIndex)),
		bundles:                 record.NewCache[bundleCodec, *bundleIndex, bundleObject, *datastore.ListBundlesRequest](store, "bundle", new(bundleIndex)),
		entries:                 record.NewCache[entryCodec, *entryIndex, entryObject, *datastore.ListRegistrationEntriesRequest](store, "entry", new(entryIndex)),
		joinTokens:              record.NewCache[joinTokenCodec, *joinTokenIndex, joinTokenObject, *listJoinTokens](store, "joinToken", new(joinTokenIndex)),
		federationRelationships: record.NewCache[federationRelationshipCodec, *federationRelationshipIndex, federationRelationshipObject, *datastore.ListFederationRelationshipsRequest](store, "federationRelationship", new(federationRelationshipIndex)),
	}
	ds.watch, err = record.Watch(ctx, store, []record.Sink{
		ds.agents,
		ds.bundles,
		ds.entries,
		ds.joinTokens,
		ds.federationRelationships,
	})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (ds *DataStore) Close() error {
	var closeErr error

	if err := ds.watch.Close(); err != nil {
		if closeErr != nil {
			closeErr = err
		}
	}

	if err := ds.store.Close(); err != nil {
		if closeErr != nil {
			closeErr = err
		}
	}

	return closeErr
}
