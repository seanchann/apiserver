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

package factory

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/blang/semver/v4"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

const sqliteBusyTimeoutMilliseconds = 5000

func sqliteDSN(c storagebackend.SqliteConfig) (string, error) {
	if strings.TrimSpace(c.DSN) == "" {
		return "", fmt.Errorf("SQLite storage requires a DSN")
	}
	parts := strings.SplitN(c.DSN, "?", 2)
	if strings.TrimSpace(parts[0]) == "" {
		return "", fmt.Errorf("SQLite storage DSN requires a database path")
	}
	values := url.Values{}
	if len(parts) == 2 {
		var err error
		values, err = url.ParseQuery(parts[1])
		if err != nil {
			return "", fmt.Errorf("invalid SQLite storage DSN")
		}
	}
	if err := normalizeSQLiteWAL(values); err != nil {
		return "", err
	}
	// Always supply a bounded native busy deadline. The short alias otherwise
	// takes precedence in sqlite3's parser and could defeat this normalization.
	busy := values.Get("_busy_timeout")
	if alias := values.Get("_timeout"); alias != "" {
		busy = alias
	}
	timeout := sqliteBusyTimeoutMilliseconds
	if busy != "" {
		v, err := strconv.Atoi(busy)
		if err != nil || v < 0 {
			return "", fmt.Errorf("invalid SQLite busy timeout")
		}
		if v > 0 {
			timeout = v
		}
	}
	values.Del("_timeout")
	values.Set("_busy_timeout", strconv.Itoa(timeout))
	// Deferred BEGIN lets SQLite wait on its first cancellable write statement.
	if lock := values.Get("_txlock"); lock != "" && lock != "deferred" {
		return "", fmt.Errorf("SQLite storage requires deferred transaction locking")
	}
	values.Set("_txlock", "deferred")
	return parts[0] + "?" + values.Encode(), nil
}

// normalizeSQLiteWAL checks every alias and value before removing aliases, so
// driver precedence or duplicate parameters cannot weaken the storage policy.
func normalizeSQLiteWAL(values url.Values) error {
	for _, key := range []string{"_journal_mode", "_journal"} {
		for _, value := range values[key] {
			if !strings.EqualFold(value, "WAL") {
				return fmt.Errorf("SQLite storage requires WAL journal mode")
			}
		}
	}
	synchronous := "FULL"
	for _, key := range []string{"_synchronous", "_sync"} {
		for _, value := range values[key] {
			switch strings.ToUpper(value) {
			case "2", "FULL":
			case "3", "EXTRA":
				synchronous = "EXTRA"
			default:
				return fmt.Errorf("SQLite storage requires FULL or EXTRA synchronization")
			}
		}
	}
	values.Del("_journal")
	values.Del("_sync")
	values.Set("_journal_mode", "WAL")
	values.Set("_synchronous", synchronous)
	return nil
}

func validateSQLiteVersion(version string) error {
	actual, err := semver.Parse(version)
	if err != nil || actual.LT(semver.Version{Major: 3, Minor: 51, Patch: 3}) {
		return fmt.Errorf("SQLite storage requires SQLite 3.51.3 or later")
	}
	return nil
}

// checkSQLitePool checks effective state on one owned native connection. The
// normalized driver DSN applies WAL and synchronization to every later connection.
func checkSQLitePool(ctx context.Context, db *sql.DB) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("SQLite storage verification connection failed")
	}
	defer connection.Close()
	var version, journal string
	var synchronous int
	if err := connection.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return fmt.Errorf("SQLite storage version query failed")
	}
	if err := validateSQLiteVersion(version); err != nil {
		return err
	}
	if err := connection.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return fmt.Errorf("SQLite storage journal query failed")
	}
	if journal != "wal" {
		return fmt.Errorf("SQLite storage requires effective WAL journal mode; memory databases are unsupported")
	}
	if err := connection.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("SQLite storage synchronization query failed")
	}
	if synchronous < 2 {
		return fmt.Errorf("SQLite storage requires effective FULL or EXTRA synchronization")
	}
	return nil
}

func newSqliteStorage(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, reverseKeyFunc storage.ReverseKeyFunc, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	return newSQLStorage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
}
