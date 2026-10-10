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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

const watchProgressInterval = 10 * time.Minute

type sqlWatcher struct {
	store                    *Store
	ctx                      context.Context
	cancel                   context.CancelFunc
	finish                   func()
	key                      string
	opts                     storage.ListOptions
	scope                    string
	cursor                   uint64
	initial, initialBookmark bool
	result                   chan watch.Event
	done                     chan struct{}
	progress                 chan struct{}
}

// Stop is safe to call concurrently and waits until this subscription releases its worker.
func (w *sqlWatcher) Stop()                          { w.cancel(); <-w.done }
func (w *sqlWatcher) ResultChan() <-chan watch.Event { return w.result }

func (w *sqlWatcher) run() {
	defer w.finish()
	defer close(w.done)
	defer close(w.result)
	defer w.cancel()
	defer func() { w.store.watchMu.Lock(); delete(w.store.watchers, w); w.store.watchMu.Unlock() }()
	if w.initial {
		if err := w.sendInitial(); err != nil {
			w.fail(err)
			return
		}
	}
	ticker := w.store.options.Clock.NewTicker(DefaultPollInterval)
	defer ticker.Stop()
	lastProgress := w.store.options.Clock.Now()
	for {
		if err := w.replay(); err != nil {
			w.fail(err)
			return
		}
		select {
		case <-w.ctx.Done():
			return
		case <-w.progress:
			// Replay first so a bookmark never overtakes events committed before the request.
			if err := w.replay(); err != nil {
				w.fail(err)
				return
			}
			if err := w.bookmark(false); err != nil {
				w.fail(err)
				return
			}
		case <-ticker.C():
			if w.opts.ProgressNotify && w.store.options.Clock.Since(lastProgress) >= watchProgressInterval {
				if err := w.replay(); err != nil {
					w.fail(err)
					return
				}
				if err := w.bookmark(false); err != nil {
					w.fail(err)
					return
				}
				lastProgress = w.store.options.Clock.Now()
			}
		}
	}
}
func (w *sqlWatcher) sendInitial() error {
	records, revision, err := w.store.readSnapshot(w.ctx, w.key, w.opts.Recursive, 0, w.cursor)
	if err != nil {
		return err
	}
	w.cursor = revision
	for _, item := range records {
		obj, _, _, err := w.store.restore(w.ctx, item.key, item.record)
		if err != nil {
			return err
		}
		matches, err := w.opts.Predicate.Matches(obj)
		if err != nil {
			return err
		}
		if matches {
			if err = w.emit(watch.Event{Type: watch.Added, Object: obj}, nil); err != nil {
				return err
			}
		}
	}
	if w.initialBookmark {
		return w.bookmark(true)
	}
	return nil
}
func (w *sqlWatcher) replay() error {
	for {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		events, cursor, err := w.store.readWatchBatch(w.ctx, w.key, w.opts.Recursive, w.cursor)
		if err != nil {
			return err
		}
		for _, e := range events {
			event, err := w.transform(e)
			if err != nil {
				return err
			}
			if event != nil {
				if err = w.emit(*event, &e.recordTime); err != nil {
					return err
				}
			}
		}
		w.cursor = cursor
		if len(events) < watchBatchSize {
			return nil
		}
	}
}
func (w *sqlWatcher) transform(e historyEvent) (*watch.Event, error) {
	var before, after runtime.Object
	var oldMatches, newMatches bool
	var err error
	if e.before.revision != 0 {
		before, _, _, err = w.store.restore(w.ctx, e.key, e.before)
		if err != nil {
			return nil, err
		}
		// Both images carry the committed event RV, including predicate exit and deletion.
		if err = w.store.versioner.UpdateObject(before, e.revision); err != nil {
			return nil, err
		}
		oldMatches, err = w.opts.Predicate.Matches(before)
		if err != nil {
			return nil, err
		}
	}
	if e.after.revision != 0 {
		after, _, _, err = w.store.restore(w.ctx, e.key, e.after)
		if err != nil {
			return nil, err
		}
		newMatches, err = w.opts.Predicate.Matches(after)
		if err != nil {
			return nil, err
		}
	}
	switch {
	case newMatches && oldMatches:
		return &watch.Event{Type: watch.Modified, Object: after}, nil
	case newMatches:
		return &watch.Event{Type: watch.Added, Object: after}, nil
	case oldMatches:
		return &watch.Event{Type: watch.Deleted, Object: before}, nil
	}
	return nil, nil
}
func (w *sqlWatcher) bookmark(initial bool) error {
	// An explicitly future starting RV is not evidence of committed progress.
	if !initial {
		current, err := w.store.GetCurrentResourceVersion(w.ctx)
		if err != nil {
			return err
		}
		if w.cursor > current {
			return nil
		}
	}
	recordTime := w.store.options.Clock.Now()
	obj := w.store.options.NewFunc()
	if err := w.store.versioner.UpdateObject(obj, w.cursor); err != nil {
		return err
	}
	if initial {
		if err := storage.AnnotateInitialEventsEndBookmark(obj); err != nil {
			return err
		}
	}
	return w.emit(watch.Event{Type: watch.Bookmark, Object: obj}, &recordTime)
}
func (w *sqlWatcher) emit(e watch.Event, recordTime *time.Time) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	// The single producer reserves the final channel slot for a terminal error.
	// Consumers may drain concurrently but can never reduce the reserved capacity.
	if len(w.result) >= watchQueueSize {
		return apierrors.NewResourceExpired("Watch consumer fell behind; relist and resume from the new resourceVersion.")
	}
	if recordTime != nil && w.opts.RecordTimestamps {
		e.Object = &timedWatchEvent{Object: e.Object, recordTime: *recordTime}
	}
	w.result <- e
	return nil
}
func (w *sqlWatcher) fail(err error) {
	if w.ctx.Err() != nil {
		return
	}
	status, ok := err.(apierrors.APIStatus)
	if !ok {
		status = apierrors.NewInternalError(err)
	}
	result := status.Status()
	w.result <- watch.Event{Type: watch.Error, Object: &result}
}

// timedWatchEvent is private to storage/cache ingestion and must be unwrapped by
// the watch cache before persisting or serializing its object to a client.
type timedWatchEvent struct {
	runtime.Object
	recordTime time.Time
}

var _ storage.WatchEventWithRecordTime = (*timedWatchEvent)(nil)

func (t *timedWatchEvent) GetObjectMeta() metav1.Object { m, _ := meta.Accessor(t.Object); return m }
func (t *timedWatchEvent) DeepCopyObject() runtime.Object {
	return &timedWatchEvent{Object: t.Object.DeepCopyObject(), recordTime: t.recordTime}
}
func (t *timedWatchEvent) RecordTime() time.Time  { return t.recordTime }
func (t *timedWatchEvent) Unwrap() runtime.Object { return t.Object }
