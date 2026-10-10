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

package framework

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

const fixtureTimeout = 10 * time.Second

// Database owns an isolated test database and its SQL connection pool.
// DSN may contain credentials and must never be logged.
type Database struct {
	DB            *sql.DB
	DSN           string
	closeOnce     sync.Once
	closeDatabase func() error
	closeErr      error
}

// Open creates an isolated real SQLite or MySQL database, failing the test on errors.
// MySQL requires SQL_STORAGE_TEST_MYSQL_DSN for a dedicated test instance,
// with no database selected and permission to create and drop test databases.
func Open(t testing.TB, backend string) *Database {
	t.Helper()
	var database *Database
	switch backend {
	case "sqlite":
		database = openSQLite(t)
	case "mysql":
		database = openMySQL(t)
	default:
		t.Fatalf("unsupported SQL fixture backend %q: expected mysql or sqlite", backend)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("SQL fixture cleanup failed (%T)", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
	defer cancel()
	if err := database.DB.PingContext(ctx); err != nil {
		t.Fatalf("%s fixture connection unavailable (%T)", backend, err)
	}
	return database
}

// Close closes the pool and removes only this fixture's database, exactly once.
func (d *Database) Close() error {
	d.closeOnce.Do(func() { d.closeErr = d.closeDatabase() })
	return d.closeErr
}

func openSQLite(t testing.TB) *Database {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "sql-storage-sqlite-")
	if err != nil {
		t.Fatalf("create SQLite fixture directory (%T)", err)
	}
	dsn := filepath.Join(directory, "storage.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		_ = os.RemoveAll(directory)
		t.Fatalf("open SQLite fixture (%T)", err)
	}
	db.SetMaxOpenConns(4)
	return &Database{DB: db, DSN: dsn, closeDatabase: func() error {
		return errors.Join(db.Close(), os.RemoveAll(directory))
	}}
}

func openMySQL(t testing.TB) *Database {
	t.Helper()
	dsn := os.Getenv("SQL_STORAGE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Fatal("MySQL fixture requires SQL_STORAGE_TEST_MYSQL_DSN for a dedicated test instance")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid MySQL fixture DSN; credentials suppressed")
	}
	if config.DBName != "" {
		t.Fatal("MySQL fixture DSN must not select an existing database")
	}
	if config.Timeout == 0 {
		config.Timeout = fixtureTimeout
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = fixtureTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = fixtureTimeout
	}
	admin, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatalf("open MySQL fixture administration connection (%T)", err)
	}
	admin.SetMaxOpenConns(1)
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		_ = admin.Close()
		t.Fatalf("generate isolated MySQL database name (%T)", err)
	}
	name := "sql_storage_test_" + hex.EncodeToString(token)
	ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
	defer cancel()
	// Only a generated hexadecimal identifier is interpolated; user input is never SQL.
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"); err != nil {
		_ = admin.Close()
		t.Fatalf("MySQL fixture service unavailable or database creation denied (%T)", err)
	}
	config.DBName = name
	fixtureDSN := config.FormatDSN()
	db, err := sql.Open("mysql", fixtureDSN)
	cleanup := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP DATABASE `"+name+"`")
		return errors.Join(dropErr, admin.Close())
	}
	if err != nil {
		_ = cleanup()
		t.Fatalf("open isolated MySQL database (%T)", err)
	}
	db.SetMaxOpenConns(4)
	return &Database{DB: db, DSN: fixtureDSN, closeDatabase: func() error {
		poolErr := db.Close()
		return errors.Join(poolErr, cleanup())
	}}
}
