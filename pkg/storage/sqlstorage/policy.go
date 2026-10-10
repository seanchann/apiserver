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
	"errors"
	"fmt"
	"time"
)

// initializePolicy establishes a persistent policy before any worker starts.
// All resources share revision/compaction metadata, so permitting independent
// windows would let one resource destroy history still promised by another.
func initializePolicy(db *sql.DB, dialect Dialect, window time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// The initializer owns transaction completion, as the live Store does.
	// Avoid an automatic request-context rollback racing factory failure cleanup.
	tx, err := conn.BeginTx(context.WithoutCancel(ctx), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := dialect.LockRevision(ctx, tx); err != nil {
		return err
	}
	var existing int64
	err = tx.QueryRowContext(ctx, "SELECT history_window FROM storage_policy WHERE id=1").Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, "INSERT INTO storage_policy(id,history_window) VALUES (1,?)", int64(window)); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if existing != int64(window) {
		return fmt.Errorf("SQL storage history window differs from persisted database policy")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
