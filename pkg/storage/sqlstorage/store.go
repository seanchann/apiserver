/********************************************************************
* Copyright (c) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************/

package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"k8s.io/utils/clock"
)

// Store supplies shared transactional storage semantics for SQLite and MySQL.
// It does not own db; its factory closes the pool after closing every Store.
type Store struct {
	db              *sql.DB
	dialect         Dialect
	options         Options
	versioner       storage.APIObjectVersioner
	ctx             context.Context
	cancel          context.CancelFunc
	compactRevision atomic.Int64
	watchMu         sync.Mutex
	watchers        map[*sqlWatcher]struct{}
	closed          bool
	active          sync.WaitGroup
	closeOnce       sync.Once
	sizeEstimation  bool
	maintenanceErr  error
}

// New initializes the versioned SQL schema and validates required serializers.
func New(db *sql.DB, dialect Dialect, options Options) (*Store, error) {
	if db == nil || dialect == nil || options.Codec == nil || options.NewFunc == nil || options.NewListFunc == nil {
		return nil, fmt.Errorf("SQL storage requires a database, dialect, codec and object constructors")
	}
	if options.ResourcePrefix == "" || options.ResourcePrefix == "/" || !strings.HasPrefix(options.ResourcePrefix, "/") {
		return nil, fmt.Errorf("resource prefix must start with / and identify a resource")
	}
	options.Prefix = strings.TrimSuffix(path.Join("/", options.Prefix), "/")
	if options.Transformer == nil {
		options.Transformer = identity.NewEncryptCheckTransformer()
	}
	if options.Clock == nil {
		options.Clock = clock.RealClock{}
	}
	if options.EventsHistoryWindow == 0 {
		options.EventsHistoryWindow = storagebackend.DefaultEventsHistoryWindow
	}
	if options.EventsHistoryWindow < 0 {
		return nil, fmt.Errorf("history window must not be negative")
	}
	if err := dialect.Initialize(context.Background(), db); err != nil {
		return nil, err
	}
	if err := initializePolicy(db, dialect, options.EventsHistoryWindow); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{db: db, dialect: dialect, options: options, ctx: ctx, cancel: cancel}
	ticker := options.Clock.NewTicker(DefaultPollInterval)
	s.active.Add(1)
	go s.runMaintenance(ticker)
	return s, nil
}

// Versioner returns the standard Kubernetes numeric resource version adapter.
func (s *Store) Versioner() storage.Versioner { return s.versioner }
