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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	driver "github.com/go-sql-driver/mysql"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
)

type dialect struct{}

// NewDialect returns the MySQL adapter; all keys are compared as raw bytes.
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
	if err := sqlstorage.InitializeMigrationMarker(ctx, db, true); err != nil {
		return err
	}
	return d.initializeSchema(ctx, db, 0)
}
func (dialect) LegacyTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND DATA_TYPE='json' ORDER BY TABLE_NAME")
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
	for _, ddl := range []string{
		"CREATE TABLE IF NOT EXISTS storage_meta (id BIGINT PRIMARY KEY, schema_version BIGINT NOT NULL, revision BIGINT NOT NULL, compact_revision BIGINT NOT NULL) ENGINE=InnoDB",
		fmt.Sprintf("INSERT INTO storage_meta (id,schema_version,revision,compact_revision) VALUES (1,%d,1,0) ON DUPLICATE KEY UPDATE id=id", initial),
		"CREATE TABLE IF NOT EXISTS storage_objects (key_digest BINARY(32) PRIMARY KEY, storage_key LONGBLOB NOT NULL, resource_prefix LONGBLOB NOT NULL, revision BIGINT NOT NULL, object LONGBLOB NOT NULL, expires_at BIGINT) ENGINE=InnoDB ROW_FORMAT=DYNAMIC",
		"CREATE TABLE IF NOT EXISTS storage_history (revision BIGINT PRIMARY KEY, key_digest BINARY(32) NOT NULL, storage_key LONGBLOB NOT NULL, resource_prefix LONGBLOB NOT NULL, change_type VARCHAR(16) NOT NULL, previous_object LONGBLOB, object LONGBLOB, previous_revision BIGINT NOT NULL, object_revision BIGINT NOT NULL, previous_expires_at BIGINT, expires_at BIGINT, committed_at BIGINT NOT NULL, INDEX storage_history_key_revision(key_digest,revision)) ENGINE=InnoDB ROW_FORMAT=DYNAMIC",
		"CREATE TABLE IF NOT EXISTS storage_policy (id BIGINT PRIMARY KEY, history_window BIGINT NOT NULL) ENGINE=InnoDB",
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	var version int
	if err := db.QueryRowContext(ctx, "SELECT schema_version FROM storage_meta WHERE id=1").Scan(&version); err != nil {
		return err
	}
	if version != initial && !(initial == 0 && version == 1) {
		return fmt.Errorf("unsupported SQL storage schema version %d", version)
	}
	return nil
}
func (dialect) LockRevision(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var revision uint64
	err := tx.QueryRowContext(ctx, "SELECT revision FROM storage_meta WHERE id=1 FOR UPDATE").Scan(&revision)
	return revision, err
}
func (dialect) IsRetryable(err error) bool {
	var e *driver.MySQLError
	return errors.As(err, &e) && (e.Number == 1205 || e.Number == 1213)
}
func (dialect) IsDuplicate(err error) bool {
	var e *driver.MySQLError
	return errors.As(err, &e) && e.Number == 1062
}

func (dialect) LegacyKeyValue() bool { return false }
