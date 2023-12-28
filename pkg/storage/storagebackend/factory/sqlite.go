/********************************************************************
* Copyright (c) 2008 - 2024. seanchann <seanchann.zhou@gmail.com>
* All rights reserved.
*
* PROPRIETARY RIGHTS of the following material in either
* electronic or paper format pertain to sean.
* All manufacturing, reproduction, use, and sales involved with
* this subject MUST conform to the license agreement signed
* with sean.
*******************************************************************/

package factory

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog/v2"
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

func newSqliteStorage(c storagebackend.Config, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
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
