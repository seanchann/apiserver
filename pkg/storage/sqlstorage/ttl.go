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

	"k8s.io/utils/clock"
)

const expirationBatchSize = 128

func (s *Store) runMaintenance(ticker clock.Ticker) {
	defer s.active.Done()
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C():
			err := s.expire(s.ctx)
			if err == nil {
				err = s.retainHistory(s.ctx)
			}
			s.watchMu.Lock()
			s.maintenanceErr = err
			s.watchMu.Unlock()
		}
	}
}

// expire snapshots candidates without a write lock. Renewals that win before
// mutate's revision comparison invalidate the old expiry and are never deleted.
func (s *Store) expire(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "SELECT storage_key,revision,object,expires_at FROM storage_objects WHERE resource_prefix=? AND expires_at<=? ORDER BY expires_at,storage_key LIMIT ?", []byte(s.storageResourcePrefix()), s.options.Clock.Now().UnixNano(), expirationBatchSize)
	if err != nil {
		return err
	}
	var candidates []snapshotRecord
	for rows.Next() {
		var candidate snapshotRecord
		var key []byte
		if err = rows.Scan(&key, &candidate.revision, &candidate.object, &candidate.expiresAt); err != nil {
			rows.Close()
			return err
		}
		candidate.key = string(key)
		candidates = append(candidates, candidate)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, candidate := range candidates {
		_, err = s.mutate(ctx, candidate.key, candidate.record, nil, sql.NullInt64{}, "DELETED", false)
		if err != nil && !errors.Is(err, errRevisionChanged) {
			return err
		}
	}
	return nil
}

// retainHistory compacts only the contiguous prefix older than the configured
// window. A later old timestamp cannot jump across a recent committed event.
func (s *Store) retainHistory(ctx context.Context) error {
	var boundary, oldest uint64
	cutoff := s.options.Clock.Now().Add(-s.options.EventsHistoryWindow).UnixNano()
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MIN(CASE WHEN committed_at>=? THEN revision END)-1,MAX(revision),0),COALESCE(MIN(CASE WHEN committed_at<? THEN revision END),0) FROM storage_history", cutoff, cutoff).Scan(&boundary, &oldest)
	if err != nil || oldest == 0 || oldest > boundary {
		return err
	}
	_, err = s.Compact(ctx, boundary)
	return err
}
