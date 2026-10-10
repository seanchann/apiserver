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
	"time"

	"k8s.io/apiserver/pkg/storage"
)

var _ storage.Interface = (*Store)(nil)

// operationContext admits under the same mutex that seals Close's wait group.
// Its cleanup belongs to the operation, never to a caller's cancellation handle.
func (s *Store) operationContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	s.watchMu.Lock()
	if s.closed {
		s.watchMu.Unlock()
		cancel()
		return ctx, cancel
	}
	s.active.Add(1)
	s.watchMu.Unlock()
	stop := context.AfterFunc(s.ctx, cancel)
	if s.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel(); s.active.Done() }
}

// Close seals admission, cancels work and waits for operations and workers to drain.
// Callers must release their callbacks; cancellation cannot forcibly terminate Go code.
// The database pool remains owned by the caller or factory.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.watchMu.Lock()
		s.closed = true
		s.cancel()
		s.watchMu.Unlock()
		s.active.Wait()
	})
	return nil
}

// ReadinessCheck validates the live schema, metadata and both data tables.
func (s *Store) ReadinessCheck() error {
	parent, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ctx, done := s.operationContext(parent)
	defer done()
	if err := ctx.Err(); err != nil {
		return err
	}
	var version int
	var revision, compacted, window int64
	var objects, history int
	err := s.db.QueryRowContext(ctx, "SELECT schema_version,revision,compact_revision,(SELECT COUNT(*) FROM storage_objects WHERE 1=0),(SELECT COUNT(*) FROM storage_history WHERE 1=0),(SELECT history_window FROM storage_policy WHERE id=1) FROM storage_meta WHERE id=1").Scan(&version, &revision, &compacted, &objects, &history, &window)
	if err != nil {
		return err
	}
	if version != 1 || revision < 1 || compacted < 0 || compacted > revision || window != int64(s.options.EventsHistoryWindow) {
		return fmt.Errorf("invalid SQL storage metadata")
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.closed {
		return context.Canceled
	}
	return s.maintenanceErr
}
