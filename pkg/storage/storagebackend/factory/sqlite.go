/*

Copyright 2018 This Project Authors.

Author:  seanchann <seanchann@foxmail.com>

See docs/ for more information about the  project.

*/

package factory

import (
	"database/sql"

	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog"
	_ "github.com/mattn/go-sqlite3"
)

func newSqliteClient(dsn string, debug bool) (*sql.DB, error) {
	var err error

	connStr := string(dsn)
	if debug {
		klog.Infof("sqlite finish connect dsn %v", dsn)
	}
	db, err := sql.Open(string("sqlite3"), connStr)
	if err != nil {
		return nil, err
	}

	return db, db.Ping()
}

func newSqliteStorage(c storagebackend.Config) (storage.Interface, DestroyFunc, error) {
	dsn := c.Sqlite.DSN

	client, err := newSqliteClient(dsn, c.Sqlite.Debug)
	if err != nil {
		return nil, nil, err
	}

	destroyFunc := func() {
		client.Close()
	}

	return sqlite.New(client, c.Codec, "v1", c.Sqlite.ListDefaultLimit), destroyFunc, nil
}
