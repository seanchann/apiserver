/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package factory

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	mysqlstorage "k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// DestroyFunc is to destroy any resources used by the storage returned in Create() together.
type DestroyFunc func()

// Create creates a storage backend based on given config.
func Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, reverseKeyFunc storage.ReverseKeyFunc, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	switch c.Type {
	case storagebackend.StorageTypeMysql:
		return newMysqlStorage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
	case storagebackend.StorageTypeSqlite:
		return newSqliteStorage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
	case storagebackend.StorageTypeETCD2:
		return nil, nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3Storage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
	default:
		return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateHealthCheck creates a healthcheck function based on given config.
func CreateHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeMysql, storagebackend.StorageTypeSqlite:
		timeout := c.HealthcheckTimeout
		if timeout == 0 {
			timeout = storagebackend.DefaultHealthcheckTimeout
		}
		return newSQLCheck(c, timeout, stopCh)
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3HealthCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeMysql, storagebackend.StorageTypeSqlite:
		timeout := c.ReadycheckTimeout
		if timeout == 0 {
			timeout = storagebackend.DefaultReadinessTimeout
		}
		return newSQLCheck(c, timeout, stopCh)
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ReadyCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateProber(c storagebackend.Config) (Prober, error) {
	switch c.Type {
	case storagebackend.StorageTypeMysql, storagebackend.StorageTypeSqlite:
		return newSQLProberMonitor(c)
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateMonitor(c storagebackend.Config) (metrics.Monitor, error) {
	switch c.Type {
	case storagebackend.StorageTypeMysql, storagebackend.StorageTypeSqlite:
		return newSQLProberMonitor(c)
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// Prober is an interface that defines the Probe function for doing etcd readiness/liveness checks.
type Prober interface {
	Probe(ctx context.Context) error
	Close() error
}

func openSQLPool(c storagebackend.Config) (*sql.DB, error) {
	var dsn, name string
	var err error
	switch c.Type {
	case storagebackend.StorageTypeMysql:
		dsn, err = mysqlDSN(c.Mysql)
		name = "mysql"
	case storagebackend.StorageTypeSqlite:
		dsn, err = sqliteDSN(c.Sqlite)
		name = "sqlite3"
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(name, dsn)
	if err != nil {
		return nil, fmt.Errorf("SQL storage connection configuration failed; credentials suppressed")
	}
	// A private in-memory SQLite database is per native connection. One connection
	// retains its schema throughout this pool's lifetime; file databases allow four.
	if c.Type == storagebackend.StorageTypeSqlite && (strings.HasPrefix(dsn, ":memory:") || strings.Contains(dsn, "mode=memory")) {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(4)
	}
	ctx, cancel := context.WithTimeout(context.Background(), sqlDriverTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("SQL storage connection failed; credentials suppressed")
	}
	return db, nil
}
func newSQLStorage(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, reverseKeyFunc storage.ReverseKeyFunc, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	db, err := openSQLPool(c.Config)
	if err != nil {
		return nil, nil, err
	}
	options := sqlstorage.Options{Prefix: c.Prefix, ResourcePrefix: resourcePrefix, Codec: c.Codec, Transformer: c.Transformer, NewFunc: newFunc, NewListFunc: newListFunc, ReverseKeyFunc: reverseKeyFunc, EventsHistoryWindow: c.EventsHistoryWindow}
	var store *sqlstorage.Store
	if c.Type == storagebackend.StorageTypeMysql {
		store, err = mysqlstorage.New(db, options)
	} else {
		store, err = sqlite.New(db, options)
	}
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	var once sync.Once
	destroy := func() { once.Do(func() { _ = store.Close(); _ = db.Close() }) }
	return store, destroy, nil
}
