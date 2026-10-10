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

package framework

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	clocktesting "k8s.io/utils/clock/testing"
)

type watchStore interface {
	Watch(context.Context, string, storage.ListOptions) (watch.Interface, error)
	RequestWatchProgress(context.Context) error
}

func requireWatchStore(t *testing.T, s *sqlstorage.Store) watchStore {
	t.Helper()
	w, ok := any(s).(watchStore)
	require.True(t, ok, "SQL Store must implement durable Watch and RequestWatchProgress")
	return w
}
func startWatch(t *testing.T, s *sqlstorage.Store, ctx context.Context, key string, opts storage.ListOptions) watch.Interface {
	t.Helper()
	w, err := requireWatchStore(t, s).Watch(ctx, key, opts)
	require.NoError(t, err)
	t.Cleanup(w.Stop)
	return w
}
func nextWatch(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case e, ok := <-w.ResultChan():
		require.True(t, ok, "watch closed before expected event")
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for durable watch event")
		return watch.Event{}
	}
}
func watchRV(t *testing.T, e watch.Event) string {
	t.Helper()
	obj := e.Object
	if wrapped, ok := obj.(storage.WatchEventWithRecordTime); ok {
		obj = wrapped.Unwrap()
	}
	a, err := meta.Accessor(obj)
	require.NoError(t, err)
	return a.GetResourceVersion()
}
func updateWatchPod(t *testing.T, s *sqlstorage.Store, value string) *example.Pod {
	t.Helper()
	out := &example.Pod{}
	require.NoError(t, s.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		p := obj.(*example.Pod)
		p.Labels["value"] = value
		return p, nil, nil
	}, nil))
	return out
}
func AssertSQLWatchListBoundary(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	list := &example.PodList{}
	require.NoError(t, s.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
	created := createContractPod(t, s, "gap", 0)
	w := startWatch(t, s, context.Background(), "/pods", storage.ListOptions{Recursive: true, ResourceVersion: list.ResourceVersion, Predicate: storage.Everything})
	e := nextWatch(t, w)
	require.Equal(t, watch.Added, e.Type)
	require.Equal(t, created, e.Object)
}
func AssertSQLWatchInitialEvents(t *testing.T, backend string) {
	for _, mode := range []string{"legacy-zero", "explicit", "explicit-nonzero", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := openContract(t, backend, nil)
			created := createContractPod(t, s, "initial", 0)
			opts := storage.ListOptions{Recursive: true, ResourceVersion: "0", Predicate: storage.Everything}
			opts.Predicate.AllowWatchBookmarks = true
			yes, no := true, false
			if mode == "explicit" || mode == "explicit-nonzero" {
				opts.SendInitialEvents = &yes
			}
			if mode == "explicit-nonzero" {
				opts.ResourceVersion = created.ResourceVersion
			}
			if mode == "disabled" {
				opts.SendInitialEvents = &no
			}
			opts.RecordTimestamps = true
			w := startWatch(t, s, context.Background(), "/pods", opts)
			if mode != "disabled" {
				e := nextWatch(t, w)
				require.Equal(t, watch.Added, e.Type)
				require.Equal(t, created, e.Object)
			}
			if mode == "explicit" || mode == "explicit-nonzero" {
				e := nextWatch(t, w)
				require.Equal(t, watch.Bookmark, e.Type)
				require.Equal(t, created.ResourceVersion, watchRV(t, e))
				obj := e.Object
				_, timed := obj.(storage.WatchEventWithRecordTime)
				require.True(t, timed, "initial completion bookmark must use the upstream timestamp wrapper")
				if v, ok := obj.(storage.WatchEventWithRecordTime); ok {
					obj = v.Unwrap()
				}
				a, err := meta.Accessor(obj)
				require.NoError(t, err)
				require.Equal(t, "true", a.GetAnnotations()[metav1.InitialEventsAnnotationKey])
			}
			changed := updateWatchPod(t, s, "after")
			e := nextWatch(t, w)
			require.Equal(t, watch.Modified, e.Type)
			require.Equal(t, changed.ResourceVersion, watchRV(t, e))
		})
	}
}
func AssertSQLWatchPredicateTransitions(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	created := createContractPod(t, s, "out", 0)
	pred := storage.SelectionPredicate{Label: labels.SelectorFromSet(labels.Set{"value": "in"}), Field: fields.Everything(), GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
		p := obj.(*example.Pod)
		return p.Labels, fields.Set{}, nil
	}}
	w := startWatch(t, s, context.Background(), "/pods/ns", storage.ListOptions{Recursive: true, ResourceVersion: created.ResourceVersion, Predicate: pred})
	require.NoError(t, s.Create(context.Background(), "/pods/ns-other/other", &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ns-other", Labels: map[string]string{"value": "in"}}}, nil, 0))
	for _, step := range []struct {
		v     string
		event watch.EventType
	}{{"in", watch.Added}, {"out", watch.Deleted}, {"in", watch.Added}} {
		out := updateWatchPod(t, s, step.v)
		e := nextWatch(t, w)
		require.Equal(t, step.event, e.Type)
		require.Equal(t, out.ResourceVersion, watchRV(t, e))
		require.Equal(t, "name", e.Object.(*example.Pod).Name)
	}
	out := &example.Pod{}
	require.NoError(t, s.Delete(context.Background(), contractKey, out, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}))
	e := nextWatch(t, w)
	require.Equal(t, watch.Deleted, e.Type)
	require.Equal(t, out.ResourceVersion, watchRV(t, e))
	require.Equal(t, created.UID, e.Object.(*example.Pod).UID)
}
func AssertSQLWatchProgressAndTimestamp(t *testing.T, backend string) {
	t.Run("decode-time-survives-predicate-delay", func(t *testing.T) {
		decodedAt := time.Unix(1800000000, 0)
		fake := clocktesting.NewFakeClock(decodedAt)
		s, _, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
		created := createContractPod(t, s, "before", 0)
		entered, release := make(chan struct{}), make(chan struct{})
		var enteredOnce, releaseOnce sync.Once
		// Release the predicate before cleanup waits for the watch worker.
		defer releaseOnce.Do(func() { close(release) })
		pred := storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.OneTermEqualSelector("metadata.name", "name"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				enteredOnce.Do(func() { close(entered); <-release })
				pod := obj.(*example.Pod)
				return pod.Labels, fields.Set{"metadata.name": pod.Name}, nil
			},
		}
		w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{
			ResourceVersion: created.ResourceVersion, Predicate: pred, RecordTimestamps: true,
		})
		changed := updateWatchPod(t, s, "after")
		require.NoError(t, requireWatchStore(t, s).RequestWatchProgress(context.Background()))
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("persisted event did not reach the predicate barrier")
		}
		fake.Step(time.Minute)
		releaseOnce.Do(func() { close(release) })
		event := nextWatch(t, w)
		require.Equal(t, watch.Modified, event.Type)
		wrapped, ok := event.Object.(storage.WatchEventWithRecordTime)
		require.True(t, ok)
		require.Equal(t, decodedAt, wrapped.RecordTime(), "decode timestamp must precede predicate processing")
		require.Equal(t, changed, wrapped.Unwrap())
		require.Equal(t, decodedAt, wrapped.DeepCopyObject().(storage.WatchEventWithRecordTime).RecordTime())
		progress := nextWatch(t, w)
		require.Equal(t, watch.Bookmark, progress.Type)
		require.Equal(t, changed.ResourceVersion, watchRV(t, progress))
		require.Equal(t, fake.Now(), progress.Object.(storage.WatchEventWithRecordTime).RecordTime())
	})
	s, _, _ := openContract(t, backend, nil)
	created := createContractPod(t, s, "before", 0)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("scope", "one"))
	opts := storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything, RecordTimestamps: true}
	w := startWatch(t, s, ctx, contractKey, opts)
	other := startWatch(t, s, metadata.NewOutgoingContext(context.Background(), metadata.Pairs("scope", "two")), contractKey, opts)
	before := time.Now()
	changed := updateWatchPod(t, s, "after")
	require.NoError(t, requireWatchStore(t, s).RequestWatchProgress(ctx))
	e := nextWatch(t, w)
	require.Equal(t, watch.Modified, e.Type)
	wrapped, ok := e.Object.(storage.WatchEventWithRecordTime)
	require.True(t, ok)
	require.False(t, wrapped.RecordTime().Before(before))
	require.Equal(t, changed, wrapped.Unwrap())
	require.Equal(t, wrapped.RecordTime(), wrapped.DeepCopyObject().(storage.WatchEventWithRecordTime).RecordTime())
	e = nextWatch(t, w)
	require.Equal(t, watch.Bookmark, e.Type)
	require.Equal(t, changed.ResourceVersion, watchRV(t, e))
	require.Equal(t, watch.Modified, nextWatch(t, other).Type)
	select {
	case e := <-other.ResultChan():
		t.Fatalf("progress escaped context scope: %v", e.Type)
	case <-time.After(250 * time.Millisecond):
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, requireWatchStore(t, s).RequestWatchProgress(canceled))
}
func AssertSQLSlowWatcher(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	created := createContractPod(t, s, "start", 0)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
	for i := 0; i < 400; i++ {
		updateWatchPod(t, s, strconv.Itoa(i))
	}
	require.Greater(t, cap(w.ResultChan()), 0)
	require.LessOrEqual(t, cap(w.ResultChan()), 256)
	require.Eventually(t, func() bool { return len(w.ResultChan()) == cap(w.ResultChan()) }, 10*time.Second, 10*time.Millisecond, "unread watcher must reach explicit terminal overflow")
	deadline := time.After(10 * time.Second)
	found := false
	for {
		select {
		case e, ok := <-w.ResultChan():
			if !ok {
				require.True(t, found, "slow watcher ended without explicit relist error")
				return
			}
			if e.Type == watch.Error {
				require.True(t, apierrors.IsResourceExpired(apierrors.FromObject(e.Object)))
				found = true
			}
		case <-deadline:
			t.Fatal("slow watcher did not terminate")
		}
	}
}
func AssertSQLWatchStopAndCancel(t *testing.T, backend string) {
	for _, mode := range []string{"stop", "cancel", "close", "fault"} {
		t.Run(mode, func(t *testing.T) {
			s, db, _ := openContract(t, backend, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := startWatch(t, s, ctx, contractKey, storage.ListOptions{ResourceVersion: "1", Predicate: storage.Everything})
			switch mode {
			case "stop":
				var wg sync.WaitGroup
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); w.Stop() }()
				}
				wg.Wait()
			case "cancel":
				cancel()
			case "close":
				require.NoError(t, s.Close())
			case "fault":
				_, err := db.DB.Exec("DROP TABLE storage_history")
				require.NoError(t, err)
				e := nextWatch(t, w)
				require.Equal(t, watch.Error, e.Type)
				require.True(t, apierrors.IsInternalError(apierrors.FromObject(e.Object)))
			}
			select {
			case _, ok := <-w.ResultChan():
				require.False(t, ok)
			case <-time.After(10 * time.Second):
				t.Fatal("watch did not close")
			}
			w.Stop()
			require.Eventually(t, func() bool { return db.DB.Stats().InUse == 0 }, time.Second, 10*time.Millisecond)
		})
	}
}
func AssertSQLWatchReconnectAcrossProcess(t *testing.T, backend string) {
	if os.Getenv("SQL_WATCH_CHILD") == backend {
		driver := "sqlite3"
		if backend == "mysql" {
			driver = "mysql"
		}
		db, err := sql.Open(driver, os.Getenv("SQL_WATCH_CHILD_DSN"))
		require.NoError(t, err)
		defer db.Close()
		s, err := sqlstorage.New(db, contractDialect(backend), contractOptions())
		require.NoError(t, err)
		defer s.Close()
		updateWatchPod(t, s, os.Getenv("SQL_WATCH_CHILD_VALUE"))
		return
	}
	s, db, options := openContract(t, backend, nil)
	created := createContractPod(t, s, "parent", 0)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
	runChild := func(v string) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLWatchReconnectAcrossProcess$", "-test.count=1")
		cmd.Env = append(os.Environ(), "SQL_WATCH_CHILD="+backend, "SQL_WATCH_CHILD_DSN="+db.DSN, "SQL_WATCH_CHILD_VALUE="+v)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "writer child failed (output size %d)", len(output))
		t.Logf("%s writer subprocess pid=%d completed value=%s", backend, cmd.ProcessState.Pid(), v)
	}
	runChild("first")
	first := nextWatch(t, w)
	require.Equal(t, watch.Modified, first.Type)
	rv := watchRV(t, first)
	w.Stop()
	require.NoError(t, s.Close())
	runChild("disconnected")
	reopened, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer reopened.Close()
	resumed := startWatch(t, reopened, context.Background(), contractKey, storage.ListOptions{ResourceVersion: rv, Predicate: storage.Everything})
	e := nextWatch(t, resumed)
	require.Equal(t, watch.Modified, e.Type)
	expectedState := readContractPod(t, reopened)
	stateAfterReconnect := e.Object.(*example.Pod)
	require.Equal(t, expectedState, stateAfterReconnect)
	revision, err := strconv.ParseUint(expectedState.ResourceVersion, 10, 64)
	require.NoError(t, err)
	_, err = reopened.Compact(context.Background(), revision)
	require.NoError(t, err)
	expired := startWatch(t, reopened, context.Background(), contractKey, storage.ListOptions{ResourceVersion: rv, Predicate: storage.Everything})
	e = nextWatch(t, expired)
	require.Equal(t, watch.Error, e.Type)
	require.True(t, apierrors.IsResourceExpired(apierrors.FromObject(e.Object)))
	list := &example.PodList{}
	require.NoError(t, reopened.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
	require.Equal(t, []example.Pod{*expectedState}, list.Items)
	t.Log(fmt.Sprintf("%s reconnect replay and expired-relist state verified", backend))
}

// AssertSQLWatchFutureProgress prevents bookmarks from claiming unseen future revisions.
func AssertSQLWatchFutureProgress(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	current, err := s.GetCurrentResourceVersion(context.Background())
	require.NoError(t, err)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: strconv.FormatUint(current+10, 10), Predicate: storage.Everything})
	require.NoError(t, requireWatchStore(t, s).RequestWatchProgress(context.Background()))
	select {
	case e := <-w.ResultChan():
		t.Fatalf("watch on future RV emitted premature event %v rv %s", e.Type, watchRV(t, e))
	case <-time.After(250 * time.Millisecond):
	}
}

// AssertSQLWatchSnapshotBoundary commits during initial delivery, after the durable snapshot read.
func AssertSQLWatchSnapshotBoundary(t *testing.T, backend string) {
	tr := newContractTransformer(t)
	s, _, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Transformer = tr })
	original := createContractPod(t, s, "initial", 0)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	pred := storage.SelectionPredicate{Label: labels.SelectorFromSet(labels.Set{"value": "initial"}), Field: fields.Everything(), GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
		once.Do(func() { close(entered); <-release })
		p := obj.(*example.Pod)
		return p.Labels, nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cleanup must unblock the predicate even when an assertion fails.
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	yes := true
	pred.AllowWatchBookmarks = true
	w, err := requireWatchStore(t, s).Watch(ctx, contractKey, storage.ListOptions{ResourceVersion: "0", Predicate: pred, SendInitialEvents: &yes})
	require.NoError(t, err)
	defer func() { releaseOnce.Do(func() { close(release) }); w.Stop() }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot delivery did not reach predicate")
	}
	changed := updateWatchPod(t, s, "after")
	releaseOnce.Do(func() { close(release) })
	initial := nextWatch(t, w)
	require.Equal(t, watch.Added, initial.Type)
	require.Equal(t, original, initial.Object)
	bookmark := nextWatch(t, w)
	require.Equal(t, watch.Bookmark, bookmark.Type)
	require.Equal(t, original.ResourceVersion, watchRV(t, bookmark))
	deleted := nextWatch(t, w)
	require.Equal(t, watch.Deleted, deleted.Type)
	require.Equal(t, changed.ResourceVersion, watchRV(t, deleted))
	require.False(t, tr.wrongAAD.Load())
}

// AssertSQLWatchGlobalGaps checks a cursor crosses resource gaps without pretending a later snapshot.
func AssertSQLWatchGlobalGaps(t *testing.T, backend string) {
	s, db, opts := openContract(t, backend, nil)
	current, err := s.GetCurrentResourceVersion(context.Background())
	require.NoError(t, err)
	w := startWatch(t, s, context.Background(), "/pods", storage.ListOptions{Recursive: true, ResourceVersion: strconv.FormatUint(current, 10), Predicate: storage.Everything})
	opts.ResourcePrefix = "/services"
	other, err := sqlstorage.New(db.DB, contractDialect(backend), opts)
	require.NoError(t, err)
	defer other.Close()
	require.NoError(t, other.Create(context.Background(), "/services/ns/one", contractPod("other"), nil, 0))
	boundary, err := s.GetCurrentResourceVersion(context.Background())
	require.NoError(t, err)
	require.NoError(t, requireWatchStore(t, s).RequestWatchProgress(context.Background()))
	e := nextWatch(t, w)
	require.Equal(t, watch.Bookmark, e.Type)
	require.Equal(t, strconv.FormatUint(boundary, 10), watchRV(t, e))
	_, err = s.Compact(context.Background(), boundary)
	require.NoError(t, err)
	created := createContractPod(t, s, "visible", 0)
	e = nextWatch(t, w)
	require.Equal(t, watch.Added, e.Type)
	require.Equal(t, created, e.Object)
}

// AssertSQLWatchPeriodicProgress uses fake time only; all progress revisions come from the database.
func AssertSQLWatchPeriodicProgress(t *testing.T, backend string) {
	fake := clocktesting.NewFakeClock(time.Now())
	s, _, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
	created := createContractPod(t, s, "clock", 0)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything, ProgressNotify: true})
	require.NoError(t, requireWatchStore(t, s).RequestWatchProgress(context.Background()))
	require.Equal(t, watch.Bookmark, nextWatch(t, w).Type)
	fake.Step(10 * time.Minute)
	e := nextWatch(t, w)
	require.Equal(t, watch.Bookmark, e.Type)
	require.Equal(t, created.ResourceVersion, watchRV(t, e))
}
