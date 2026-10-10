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
	"encoding/json"
	"math"
	"time"

	"google.golang.org/grpc/metadata"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
)

const watchBatchSize = 128
const watchQueueSize = 128

// Watch connects a consistent initial snapshot or explicit revision to durable history.
func (s *Store) Watch(parent context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	prefix, err := s.prepareKey(key, opts.Recursive)
	if err != nil {
		return nil, err
	}
	revision, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	if revision > math.MaxInt64 {
		return nil, apierrors.NewBadRequest("resource version exceeds SQL revision range")
	}
	// Match the upstream Watch contract: an already canceled caller receives
	// an empty closed stream, while a closed store still rejects admission.
	if parent.Err() != nil {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		return watch.NewEmptyWatch(), nil
	}
	admitted, finish := s.operationContext(parent)
	ctx, cancel := context.WithCancel(admitted)
	w := &sqlWatcher{store: s, ctx: ctx, cancel: cancel, finish: finish, key: prefix, opts: opts, cursor: revision, scope: watchScope(parent), result: make(chan watch.Event, watchQueueSize+1), done: make(chan struct{}), progress: make(chan struct{}, 1)}
	w.initial = opts.SendInitialEvents == nil && revision == 0
	if utilfeature.DefaultFeatureGate.Enabled(features.WatchList) && opts.SendInitialEvents != nil {
		w.initial = *opts.SendInitialEvents
		w.initialBookmark = w.initial && opts.Predicate.AllowWatchBookmarks
	}
	// A stream without initial events and without an explicit RV starts at the
	// current durable boundary, obtained before returning registration to its caller.
	if !w.initial && revision == 0 {
		w.cursor, err = s.GetCurrentResourceVersion(ctx)
		if err != nil {
			cancel()
			finish()
			return nil, err
		}
	}
	s.watchMu.Lock()
	if s.ctx.Err() != nil {
		s.watchMu.Unlock()
		cancel()
		finish()
		return nil, s.ctx.Err()
	}
	if ctx.Err() != nil {
		s.watchMu.Unlock()
		cancel()
		finish()
		return watch.NewEmptyWatch(), nil
	}
	if s.watchers == nil {
		s.watchers = make(map[*sqlWatcher]struct{})
	}
	s.watchers[w] = struct{}{}
	s.watchMu.Unlock()
	go w.run()
	utilflowcontrol.WatchInitialized(parent)
	return w, nil
}

func watchScope(ctx context.Context) string {
	md, _ := metadata.FromOutgoingContext(ctx)
	if len(md) == 0 {
		return ""
	}
	data, _ := json.Marshal(md)
	return string(data)
}

// RequestWatchProgress schedules ordered progress on subscriptions with matching
// outgoing gRPC metadata, including subscriptions that did not request periodic progress.
func (s *Store) RequestWatchProgress(parent context.Context) error {
	ctx, cancel := s.operationContext(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	scope := watchScope(ctx)
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for w := range s.watchers {
		if w.scope == scope {
			select {
			case w.progress <- struct{}{}:
			default:
			}
		}
	}
	return nil
}

type historyEvent struct {
	recordTime    time.Time
	key           string
	revision      uint64
	before, after record
}

// readWatchBatch uses one repeatable-read boundary for both compaction checks and
// history. On an exhausted batch the global cursor advances even across unrelated
// resource writes; a full batch advances only through the last returned row.
func (s *Store) readWatchBatch(ctx context.Context, key string, recursive bool, cursor uint64) ([]historyEvent, uint64, error) {
	tx, release, err := s.beginTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, cursor, err
	}
	defer release()
	var current, compacted uint64
	if err = tx.QueryRowContext(ctx, "SELECT revision,compact_revision FROM storage_meta WHERE id=1").Scan(&current, &compacted); err != nil {
		return nil, cursor, err
	}
	s.observeCompaction(compacted)
	if cursor < compacted {
		return nil, cursor, apierrors.NewResourceExpired("Watch history has expired; relist and resume from the new resourceVersion.")
	}
	query := "SELECT storage_key,revision,previous_revision,previous_object,object_revision,object FROM storage_history WHERE resource_prefix=? AND revision>? AND revision<=?"
	args := []any{[]byte(s.storageResourcePrefix()), cursor, current}
	if recursive {
		query += " AND SUBSTR(storage_key,1,?)=?"
		args = append(args, len(key), []byte(key))
	} else {
		query += " AND storage_key=?"
		args = append(args, []byte(key))
	}
	query += " ORDER BY revision LIMIT ?"
	args = append(args, watchBatchSize)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, cursor, err
	}
	events := make([]historyEvent, 0, watchBatchSize)
	for rows.Next() {
		var e historyEvent
		var keyBytes []byte
		if err = rows.Scan(&keyBytes, &e.revision, &e.before.revision, &e.before.object, &e.after.revision, &e.after.object); err != nil {
			rows.Close()
			return nil, cursor, err
		}
		// Preserve persisted-event decode time across transforms and predicates.
		e.recordTime = s.options.Clock.Now()
		e.key = string(keyBytes)
		events = append(events, e)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, cursor, err
	}
	if closeErr != nil {
		return nil, cursor, closeErr
	}
	if err = s.commitTransaction(ctx, tx); err != nil {
		return nil, cursor, err
	}
	if len(events) == watchBatchSize {
		cursor = events[len(events)-1].revision
	} else if current > cursor {
		cursor = current
	}
	return events, cursor, nil
}
