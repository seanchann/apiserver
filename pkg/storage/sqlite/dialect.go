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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sqlite3 "github.com/mattn/go-sqlite3"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
)

type dialect struct{}

// NewDialect returns the SQLite adapter; the factory owns its database pool.
func NewDialect() sqlstorage.Dialect { return dialect{} }

func (d dialect) Initialize(ctx context.Context, db *sql.DB) error {
	tables, err := d.LegacyTables(ctx, db)
	if err != nil {
		return err
	}
	if len(tables) > 0 {
		if err = sqlstorage.CheckLegacyReady(ctx, db); err != nil {
			return err
		}
	}
	return d.initializeSchema(ctx, db, 1)
}
func (d dialect) InitializeMigration(ctx context.Context, db *sql.DB) error {
	if err := sqlstorage.InitializeMigrationMarker(ctx, db, false); err != nil {
		return err
	}
	return d.initializeSchema(ctx, db, 0)
}
func (dialect) LegacyTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='keyval'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}
func (dialect) initializeSchema(ctx context.Context, db *sql.DB, initial int) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	tx, err := connection.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, ddl := range []string{
		"CREATE TABLE IF NOT EXISTS storage_meta (id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL, revision INTEGER NOT NULL, compact_revision INTEGER NOT NULL)",
		fmt.Sprintf("INSERT OR IGNORE INTO storage_meta (id,schema_version,revision,compact_revision) VALUES (1,%d,1,0)", initial),
		"CREATE TABLE IF NOT EXISTS storage_objects (key_digest BLOB PRIMARY KEY NOT NULL, storage_key BLOB NOT NULL, resource_prefix BLOB NOT NULL, revision INTEGER NOT NULL, object BLOB NOT NULL, expires_at INTEGER)",
		"CREATE TABLE IF NOT EXISTS storage_history (revision INTEGER PRIMARY KEY, key_digest BLOB NOT NULL, storage_key BLOB NOT NULL, resource_prefix BLOB NOT NULL, change_type TEXT NOT NULL, previous_object BLOB, object BLOB, previous_revision INTEGER NOT NULL, object_revision INTEGER NOT NULL, previous_expires_at INTEGER, expires_at INTEGER, committed_at INTEGER NOT NULL)",
		"CREATE INDEX IF NOT EXISTS storage_history_key_revision ON storage_history(key_digest,revision)",
		"CREATE TABLE IF NOT EXISTS storage_policy (id INTEGER PRIMARY KEY CHECK(id=1), history_window INTEGER NOT NULL)",
	} {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	var version int
	if err := tx.QueryRowContext(ctx, "SELECT schema_version FROM storage_meta WHERE id=1").Scan(&version); err != nil {
		return err
	}
	if version != initial && !(initial == 0 && version == 1) {
		return fmt.Errorf("unsupported SQL storage schema version %d", version)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
func (dialect) LockRevision(ctx context.Context, tx *sql.Tx) (uint64, error) {
	// SQLite's first statement must acquire the write lock, avoiding read-to-write upgrades.
	if _, err := tx.ExecContext(ctx, "UPDATE storage_meta SET revision=revision WHERE id=1"); err != nil {
		return 0, err
	}
	var revision uint64
	err := tx.QueryRowContext(ctx, "SELECT revision FROM storage_meta WHERE id=1").Scan(&revision)
	return revision, err
}
func (dialect) IsRetryable(err error) bool {
	var e sqlite3.Error
	return errors.As(err, &e) && (e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked)
}
func (dialect) IsDuplicate(err error) bool {
	var e sqlite3.Error
	return errors.As(err, &e) && (e.ExtendedCode == sqlite3.ErrConstraintPrimaryKey || e.ExtendedCode == sqlite3.ErrConstraintUnique)
}

func (dialect) LegacyKeyValue() bool { return true }
