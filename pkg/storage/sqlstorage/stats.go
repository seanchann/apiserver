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
	"fmt"

	"k8s.io/apiserver/pkg/storage"
)

// EnableResourceSizeEstimation enables exact serialized-byte averaging.
// SQL can aggregate the persisted objects directly instead of sampling KeysFunc.
func (s *Store) EnableResourceSizeEstimation(getKeys storage.KeysFunc) error {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.closed {
		return context.Canceled
	}
	if getKeys == nil {
		return fmt.Errorf("KeysFunc cannot be nil")
	}
	if s.sizeEstimation {
		return fmt.Errorf("resource size estimation already enabled")
	}
	s.sizeEstimation = true
	return nil
}

// Stats counts this resource's live rows and optionally averages transformed bytes.
func (s *Store) Stats(parent context.Context) (storage.Stats, error) {
	ctx, done := s.operationContext(parent)
	defer done()
	if err := ctx.Err(); err != nil {
		return storage.Stats{}, err
	}
	s.watchMu.Lock()
	enabled := s.sizeEstimation
	s.watchMu.Unlock()
	var result storage.Stats
	var total int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(LENGTH(object)),0) FROM storage_objects WHERE resource_prefix=?", []byte(s.storageResourcePrefix())).Scan(&result.ObjectCount, &total)
	if err != nil {
		return storage.Stats{}, err
	}
	if enabled && result.ObjectCount > 0 {
		result.EstimatedAverageObjectSizeBytes = total / result.ObjectCount
	}
	return result, nil
}
