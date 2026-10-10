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
	"time"
)

func (s *Store) observeCompaction(revision uint64) {
	for {
		previous := s.compactRevision.Load()
		if previous >= int64(revision) || s.compactRevision.CompareAndSwap(previous, int64(revision)) {
			return
		}
	}
}

// CompactRevision returns the latest observed completed boundary. A database
// outage preserves the last observation because the upstream API has no error result.
func (s *Store) CompactRevision() int64 {
	parent, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	if ctx.Err() != nil {
		return s.compactRevision.Load()
	}
	var revision uint64
	if err := s.db.QueryRowContext(ctx, "SELECT compact_revision FROM storage_meta WHERE id=1").Scan(&revision); err == nil {
		s.observeCompaction(revision)
	}
	return s.compactRevision.Load()
}

// Compact discards obsolete history through before, retaining the most recent
// image at or before the boundary for every complete key, including tombstones.
// Requested boundaries beyond the committed revision clamp to the current revision.
func (s *Store) Compact(ctx context.Context, before uint64) (uint64, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	for {
		boundary, err := s.compactOnce(ctx, before)
		if err == nil {
			s.observeCompaction(boundary)
			return boundary, nil
		}
		if !s.dialect.IsRetryable(err) {
			return 0, err
		}
		timer := time.NewTimer(transactionRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}
func (s *Store) compactOnce(ctx context.Context, before uint64) (uint64, error) {
	tx, release, err := s.beginTransaction(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer release()
	current, err := s.dialect.LockRevision(ctx, tx)
	if err != nil {
		return 0, err
	}
	var boundary uint64
	if err = tx.QueryRowContext(ctx, "SELECT compact_revision FROM storage_meta WHERE id=1").Scan(&boundary); err != nil {
		return 0, err
	}
	if before > current {
		before = current
	}
	if before <= boundary {
		return boundary, s.commitTransaction(ctx, tx)
	}
	// Select then delete by immutable revision to avoid MySQL's target-table
	// subquery restriction. Full keys qualify digest matches to preserve identity.
	rows, err := tx.QueryContext(ctx, "SELECT h.revision FROM storage_history h WHERE h.revision<=? AND EXISTS (SELECT 1 FROM storage_history n WHERE n.key_digest=h.key_digest AND n.storage_key=h.storage_key AND n.revision<=? AND n.revision>h.revision)", before, before)
	if err != nil {
		return 0, err
	}
	var obsolete []uint64
	for rows.Next() {
		var revision uint64
		if err = rows.Scan(&revision); err != nil {
			rows.Close()
			return 0, err
		}
		obsolete = append(obsolete, revision)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return 0, err
	}
	if closeErr != nil {
		return 0, closeErr
	}
	for _, revision := range obsolete {
		if _, err = tx.ExecContext(ctx, "DELETE FROM storage_history WHERE revision=?", revision); err != nil {
			return 0, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET compact_revision=? WHERE id=1", before); err != nil {
		return 0, err
	}
	if err = s.commitTransaction(ctx, tx); err != nil {
		return 0, err
	}
	return before, nil
}
