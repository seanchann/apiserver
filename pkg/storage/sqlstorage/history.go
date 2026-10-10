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
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/storage"
)

type snapshotRecord struct {
	key string
	record
}

// GetCurrentResourceVersion reads the database-wide committed revision, including empty stores.
func (s *Store) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var revision uint64
	err := s.db.QueryRowContext(ctx, "SELECT revision FROM storage_meta WHERE id=1").Scan(&revision)
	return revision, err
}

func (s *Store) readSnapshot(ctx context.Context, prefix string, recursive bool, revision, minimum uint64) ([]snapshotRecord, uint64, error) {
	// Repeatable read keeps the metadata boundary and rows atomic against compaction.
	tx, release, err := s.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer release()
	var current, compacted uint64
	if err = tx.QueryRowContext(ctx, "SELECT revision,compact_revision FROM storage_meta WHERE id=1").Scan(&current, &compacted); err != nil {
		return nil, 0, err
	}
	s.observeCompaction(compacted)
	if minimum > current {
		return nil, 0, storage.NewTooLargeResourceVersionError(minimum, current, 0)
	}
	if revision == 0 {
		revision = current
	}
	if revision > current {
		return nil, 0, storage.NewTooLargeResourceVersionError(revision, current, 0)
	}
	if revision < compacted {
		return nil, 0, apierrors.NewResourceExpired("The resourceVersion for the provided list is too old.")
	}
	query := "SELECT h.storage_key,h.object_revision,h.object,h.expires_at FROM storage_history h WHERE h.resource_prefix=? AND h.revision<=? AND h.object_revision>0 AND NOT EXISTS (SELECT 1 FROM storage_history n WHERE n.key_digest=h.key_digest AND n.storage_key=h.storage_key AND n.revision<=? AND n.revision>h.revision)"
	args := []any{[]byte(s.storageResourcePrefix()), revision, revision}
	if recursive {
		query += " AND SUBSTR(h.storage_key,1,?)=?"
		args = append(args, len(prefix), []byte(prefix))
	} else {
		query += " AND h.storage_key=?"
		args = append(args, []byte(prefix))
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	records := []snapshotRecord{}
	for rows.Next() {
		var item snapshotRecord
		var key []byte
		if err = rows.Scan(&key, &item.revision, &item.object, &item.expiresAt); err != nil {
			rows.Close()
			return nil, 0, err
		}
		item.key = string(key)
		records = append(records, item)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, 0, err
	}
	if closeErr != nil {
		return nil, 0, closeErr
	}
	if err = s.commitTransaction(ctx, tx); err != nil {
		return nil, 0, err
	}
	// MySQL BLOB sorting may compare only max_sort_length bytes. Go compares full
	// keys, including long suffixes and invalid UTF-8. Each page materializes O(N)
	// snapshot rows; a future optimization must preserve this bytewise ordering.
	sort.Slice(records, func(i, j int) bool { return records[i].key < records[j].key })
	return records, revision, nil
}
