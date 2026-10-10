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
	"errors"
	"os"
	"os/exec"
	goruntime "runtime"
	"strconv"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
)

type lifecycleAPI interface {
	Stats(context.Context) (storage.Stats, error)
	ReadinessCheck() error
	EnableResourceSizeEstimation(storage.KeysFunc) error
}

func requireLifecycle(t *testing.T, s any) lifecycleAPI {
	t.Helper()
	api, ok := s.(lifecycleAPI)
	require.True(t, ok, "SQL store is missing lifecycle/statistics methods (capability failure)")
	return api
}

// AssertSQLTTLUpdateRace starts with a real-time expiry to distinguish behavior from missing APIs.
func AssertSQLTTLUpdateRace(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	created := createContractPod(t, s, "expires", 1)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
	select {
	case event := <-w.ResultChan():
		require.Equal(t, watch.Deleted, event.Type)
		require.Equal(t, created.UID, event.Object.(*example.Pod).UID)
		require.NotEqual(t, created.ResourceVersion, event.Object.(*example.Pod).ResourceVersion)
	case <-time.After(3 * time.Second):
		t.Fatal("persisted TTL did not produce a committed deletion")
	}
}

// AssertSQLStatsAndReadiness scopes statistics to live rows and validates failure readiness.
func AssertSQLStatsAndReadiness(t *testing.T, backend string) {
	s, db, _ := openContract(t, backend, nil)
	api := requireLifecycle(t, s)
	require.NoError(t, api.ReadinessCheck())
	createContractPod(t, s, "count", 0)
	require.NoError(t, api.EnableResourceSizeEstimation(func(context.Context) ([]string, error) { return []string{contractKey}, nil }))
	stats, err := api.Stats(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ObjectCount)
	var bytes int64
	require.NoError(t, db.DB.QueryRow("SELECT LENGTH(object) FROM storage_objects WHERE storage_key=?", []byte(contractKey)).Scan(&bytes))
	require.Equal(t, bytes, stats.EstimatedAverageObjectSizeBytes)
	require.Error(t, api.EnableResourceSizeEstimation(nil))
	require.Error(t, api.EnableResourceSizeEstimation(func(context.Context) ([]string, error) { return nil, nil }))
	require.NoError(t, s.Close())
	require.Error(t, api.ReadinessCheck())
	_, err = api.Stats(context.Background())
	require.Error(t, err)
	require.NoError(t, db.DB.Ping())
}

// AssertSQLDestroyDuringActivity proves cancellation precedes drain completion.
func AssertSQLDestroyDuringActivity(t *testing.T, backend string) {
	s, db, _ := openContract(t, backend, nil)
	createContractPod(t, s, "original", 0)
	before := snapshotContractDB(t, db.DB)
	w := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: "1", Predicate: storage.Everything})
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- s.Delete(context.Background(), contractKey, &example.Pod{}, nil, func(ctx context.Context, _ runtime.Object) error {
			entered <- ctx
			<-release
			return nil
		}, nil, storage.DeleteOptions{})
	}()
	var ctx context.Context
	select {
	case ctx = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("delete callback did not enter")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel admitted callback context")
	}
	select {
	case <-closed:
		t.Error("Close returned while an admitted callback was still active")
		closed <- nil
	case <-time.After(100 * time.Millisecond):
	}
	// Release once; deferred release also handles assertion failures.
	unblock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not drain")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not drain")
	}
	w.Stop()
	require.NoError(t, s.Close())
	require.NoError(t, db.DB.Ping())
	require.Zero(t, db.DB.Stats().InUse)
	require.Equal(t, before, snapshotContractDB(t, db.DB))
}

// maintenanceClock replaces time only and acknowledges each completed worker cycle.
// Watch tickers remain ordinary fake-clock tickers; event assertions use durable replay.
type maintenanceClock struct {
	*clocktesting.FakeClock
	mu     sync.Mutex
	ticker *maintenanceTicker
}
type maintenanceTicker struct {
	ticks   chan time.Time
	ready   chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func newMaintenanceClock(now time.Time) *maintenanceClock {
	return &maintenanceClock{FakeClock: clocktesting.NewFakeClock(now)}
}
func (c *maintenanceClock) NewTicker(d time.Duration) clock.Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ticker != nil {
		return c.FakeClock.NewTicker(d)
	}
	c.ticker = &maintenanceTicker{ticks: make(chan time.Time), ready: make(chan struct{}, 1), stopped: make(chan struct{})}
	return c.ticker
}
func (t *maintenanceTicker) C() <-chan time.Time {
	select {
	case t.ready <- struct{}{}:
	default:
	}
	return t.ticks
}
func (t *maintenanceTicker) Stop() { t.once.Do(func() { close(t.stopped) }) }
func waitSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}
func (c *maintenanceClock) wait(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	ticker := c.ticker
	c.mu.Unlock()
	require.NotNil(t, ticker, "maintenance ticker was not constructed")
	waitSignal(t, ticker.ready, "maintenance worker did not reach idle barrier")
}
func (c *maintenanceClock) tick(t *testing.T) {
	t.Helper()
	select {
	case c.ticker.ticks <- c.Now():
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance worker did not accept tick")
	}
}
func (c *maintenanceClock) cycle(t *testing.T) { t.Helper(); c.tick(t); c.wait(t) }

// revisionBarrier intercepts only the transaction boundary, never storage results.
type revisionBarrier struct {
	sqlstorage.Dialect
	mu      sync.Mutex
	armed   bool
	entered chan context.Context
	release chan struct{}
}

func (d *revisionBarrier) LockRevision(ctx context.Context, tx *sql.Tx) (uint64, error) {
	d.mu.Lock()
	armed := d.armed
	d.armed = false
	d.mu.Unlock()
	if armed {
		d.entered <- ctx
		select {
		case <-d.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return d.Dialect.LockRevision(ctx, tx)
}

// AssertSQLTTLDeterministic verifies preserved/removed/renewed TTL, restart and CAS races.
func AssertSQLTTLDeterministic(t *testing.T, backend string) {
	t.Run("nil-zero-renew-restart-snapshot", func(t *testing.T) {
		fake := newMaintenanceClock(time.Unix(1800000000, 0))
		s, db, options := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
		fake.wait(t)
		created := createContractPod(t, s, "ttl", 10)
		var expiry int64
		require.NoError(t, db.DB.QueryRow("SELECT expires_at FROM storage_objects").Scan(&expiry))
		fake.Step(3 * time.Second)
		out := &example.Pod{}
		require.NoError(t, s.GuaranteedUpdate(t.Context(), contractKey, out, false, nil, func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
			require.EqualValues(t, 7, meta.TTL)
			obj.(*example.Pod).Labels["updated"] = "yes"
			return obj, nil, nil
		}, nil))
		var retained int64
		require.NoError(t, db.DB.QueryRow("SELECT expires_at FROM storage_objects").Scan(&retained))
		require.Equal(t, expiry, retained)
		zero := uint64(0)
		require.NoError(t, s.GuaranteedUpdate(t.Context(), contractKey, out, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return obj, &zero, nil
		}, nil))
		fake.Step(10 * time.Second)
		fake.cycle(t)
		require.Equal(t, created.UID, readContractPod(t, s).UID)
		ttl := uint64(5)
		require.NoError(t, s.GuaranteedUpdate(t.Context(), contractKey, out, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return obj, &ttl, nil
		}, nil))
		snapshot := &example.PodList{}
		require.NoError(t, s.GetList(t.Context(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, snapshot))
		require.NoError(t, s.Close())
		waitSignal(t, fake.ticker.stopped, "worker ticker was not stopped")
		restartedClock := newMaintenanceClock(fake.Now().Add(6 * time.Second))
		options.Clock = restartedClock
		restarted, err := sqlstorage.New(db.DB, contractDialect(backend), options)
		require.NoError(t, err)
		defer restarted.Close()
		restartedClock.wait(t)
		restartedClock.cycle(t)
		require.True(t, storage.IsNotFound(restarted.Get(t.Context(), contractKey, storage.GetOptions{}, &example.Pod{})))
		exact := &example.PodList{}
		require.NoError(t, restarted.GetList(t.Context(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything, ResourceVersion: snapshot.ResourceVersion, ResourceVersionMatch: metav1.ResourceVersionMatchExact}, exact))
		require.Equal(t, snapshot.Items, exact.Items)
		var deletions int
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_history WHERE change_type='DELETED'").Scan(&deletions))
		require.Equal(t, 1, deletions)
		watcher := startWatch(t, restarted, t.Context(), contractKey, storage.ListOptions{ResourceVersion: out.ResourceVersion, Predicate: storage.Everything})
		require.Equal(t, watch.Deleted, nextWatch(t, watcher).Type)
		require.NoError(t, restarted.RequestWatchProgress(t.Context()))
		require.Equal(t, watch.Bookmark, nextWatch(t, watcher).Type)
	})
	t.Run("renewal-wins-candidate-race", func(t *testing.T) {
		db := Open(t, backend)
		fake := newMaintenanceClock(time.Unix(1800000000, 0))
		options := contractOptions()
		options.Clock = fake
		gate := &revisionBarrier{Dialect: contractDialect(backend), entered: make(chan context.Context, 1), release: make(chan struct{})}
		s, err := sqlstorage.New(db.DB, gate, options)
		require.NoError(t, err)
		defer s.Close()
		fake.wait(t)
		createContractPod(t, s, "renew", 1)
		otherClock := newMaintenanceClock(fake.Now())
		otherOptions := options
		otherOptions.Clock = otherClock
		other, err := sqlstorage.New(db.DB, contractDialect(backend), otherOptions)
		require.NoError(t, err)
		defer other.Close()
		otherClock.wait(t)
		gate.mu.Lock()
		gate.armed = true
		gate.mu.Unlock()
		fake.Step(2 * time.Second)
		otherClock.Step(2 * time.Second)
		fake.tick(t)
		select {
		case <-gate.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("expiry CAS boundary not reached")
		}
		ttl := uint64(10)
		renewed := &example.Pod{}
		require.NoError(t, other.GuaranteedUpdate(t.Context(), contractKey, renewed, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return obj, &ttl, nil
		}, nil))
		close(gate.release)
		fake.wait(t)
		require.Equal(t, renewed, readContractPod(t, s))
		var deletions int
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_history WHERE change_type='DELETED'").Scan(&deletions))
		require.Zero(t, deletions)
		fake.Step(11 * time.Second)
		fake.cycle(t)
		require.True(t, storage.IsNotFound(s.Get(t.Context(), contractKey, storage.GetOptions{}, &example.Pod{})))
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_history WHERE change_type='DELETED'").Scan(&deletions))
		require.Equal(t, 1, deletions)
	})
}

// AssertSQLHistoryRetention uses timestamps and validates explicit snapshot expiry and baselines.
func AssertSQLHistoryRetention(t *testing.T, backend string) {
	fake := newMaintenanceClock(time.Unix(1800000000, 0))
	s, db, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake; o.EventsHistoryWindow = 10 * time.Second })
	fake.wait(t)
	original := createContractPod(t, s, "old", 0)
	fake.Step(5 * time.Second)
	require.NoError(t, s.Create(t.Context(), "/pods/ns/second", contractPod("second"), nil, 0))
	boundary, err := s.GetCurrentResourceVersion(t.Context())
	require.NoError(t, err)
	fake.Step(5 * time.Second)
	updateWatchPod(t, s, "recent")
	fake.Step(6 * time.Second)
	fake.cycle(t)
	require.EqualValues(t, boundary, s.CompactRevision())
	old := &example.PodList{}
	err = s.GetList(t.Context(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything, ResourceVersion: original.ResourceVersion, ResourceVersionMatch: metav1.ResourceVersionMatchExact}, old)
	require.True(t, apierrors.IsResourceExpired(err))
	require.NoError(t, s.GetList(t.Context(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything, ResourceVersion: strconv.FormatUint(boundary, 10), ResourceVersionMatch: metav1.ResourceVersionMatchExact}, old))
	require.Len(t, old.Items, 2)
	var recent int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_history WHERE committed_at>=?", fake.Now().Add(-10*time.Second).UnixNano()).Scan(&recent))
	require.Equal(t, 1, recent)
}

// AssertSQLDestroyBlockedQuery proves admitted database waits cancel without closing the pool.
func AssertSQLDestroyBlockedQuery(t *testing.T, backend string) {
	fake := newMaintenanceClock(time.Now())
	s, db, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
	fake.wait(t)
	db.DB.SetMaxOpenConns(1)
	held, err := db.DB.Conn(t.Context())
	require.NoError(t, err)
	defer held.Close()
	waits := db.DB.Stats().WaitCount
	watcher := startWatch(t, s, context.Background(), contractKey, storage.ListOptions{ResourceVersion: "1", Predicate: storage.Everything})
	done := make(chan error, 1)
	go func() { done <- s.Get(context.Background(), contractKey, storage.GetOptions{}, &example.Pod{}) }()
	deadline := time.Now().Add(3 * time.Second)
	for db.DB.Stats().WaitCount < waits+2 {
		if time.Now().After(deadline) {
			t.Fatal("database query did not wait for held connection")
		}
		goruntime.Gosched()
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel blocked query")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("query did not return")
	}
	watcher.Stop()
	require.NoError(t, held.Close())
	require.NoError(t, db.DB.Ping())
	require.Zero(t, db.DB.Stats().InUse)
	require.ErrorIs(t, s.Create(t.Context(), contractKey, contractPod("closed"), nil, 0), context.Canceled)
}

// AssertSQLStatsFailures rejects missing schema, invalid metadata and closed database state.
func AssertSQLStatsFailures(t *testing.T, backend string) {
	for _, damage := range []string{"pool", "history", "objects", "metadata"} {
		t.Run(damage, func(t *testing.T) {
			s, db, _ := openContract(t, backend, nil)
			require.NoError(t, s.ReadinessCheck())
			switch damage {
			case "pool":
				require.NoError(t, db.DB.Close())
			case "history":
				_, err := db.DB.Exec("DROP TABLE storage_history")
				require.NoError(t, err)
			case "objects":
				_, err := db.DB.Exec("DROP TABLE storage_objects")
				require.NoError(t, err)
			case "metadata":
				_, err := db.DB.Exec("UPDATE storage_meta SET revision=0")
				require.NoError(t, err)
			}
			require.Error(t, s.ReadinessCheck())
		})
	}
	t.Run("prefix-and-history-excluded", func(t *testing.T) {
		s, db, options := openContract(t, backend, nil)
		createContractPod(t, s, "first", 0)
		updateWatchPod(t, s, "updated")
		options.ResourcePrefix = "/other"
		other, err := sqlstorage.New(db.DB, contractDialect(backend), options)
		require.NoError(t, err)
		defer other.Close()
		require.NoError(t, other.Create(t.Context(), "/other/ns/name", contractPod("other"), nil, 0))
		_, err = db.DB.Exec("CREATE TABLE ignored_legacy (value INTEGER)")
		require.NoError(t, err)
		_, err = db.DB.Exec("INSERT INTO ignored_legacy(value) VALUES (1)")
		require.NoError(t, err)
		stats, err := s.Stats(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 1, stats.ObjectCount)
		require.Zero(t, stats.EstimatedAverageObjectSizeBytes)
		require.NoError(t, s.EnableResourceSizeEstimation(func(context.Context) ([]string, error) {
			return nil, errors.New("exact SQL averaging does not sample keys")
		}))
		stats, err = s.Stats(t.Context())
		require.NoError(t, err)
		require.Positive(t, stats.EstimatedAverageObjectSizeBytes)
	})
}

// AssertSQLUpstreamLifecycle runs the unchanged upstream conformance assertions.
func AssertSQLUpstreamLifecycle(t *testing.T, backend string) {
	t.Run("CreateWithTTL", func(t *testing.T) {
		s, _, _ := openContract(t, backend, nil)
		storagetesting.RunTestCreateWithTTL(t.Context(), t, s)
	})
	t.Run("GuaranteedUpdateWithTTL", func(t *testing.T) {
		s, _, _ := openContract(t, backend, nil)
		storagetesting.RunTestGuaranteedUpdateWithTTL(t.Context(), t, s)
	})
	for _, enabled := range []bool{false, true} {
		t.Run("Stats-"+strconv.FormatBool(enabled), func(t *testing.T) {
			s, _, options := openContract(t, backend, nil)
			if enabled {
				require.NoError(t, s.EnableResourceSizeEstimation(func(context.Context) ([]string, error) { return nil, nil }))
			}
			storagetesting.RunTestStats(t.Context(), t, s, options.Codec, identity.NewEncryptCheckTransformer(), enabled)
		})
	}
}

// AssertSQLTTLProcessRestart proves expiration metadata survives an actual process exit.
func AssertSQLTTLProcessRestart(t *testing.T, backend string) {
	now := time.Unix(1800000000, 0)
	if os.Getenv("SQL_TTL_CHILD") == backend {
		driver := "sqlite3"
		if backend == "mysql" {
			driver = "mysql"
		}
		db, err := sql.Open(driver, os.Getenv("SQL_TTL_CHILD_DSN"))
		require.NoError(t, err)
		defer db.Close()
		options := contractOptions()
		options.Clock = newMaintenanceClock(now)
		s, err := sqlstorage.New(db, contractDialect(backend), options)
		require.NoError(t, err)
		defer s.Close()
		createContractPod(t, s, "child", 1)
		return
	}
	db := Open(t, backend)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLTTLProcessRestart$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SQL_TTL_CHILD="+backend, "SQL_TTL_CHILD_DSN="+db.DSN)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "TTL writer child failed (output bytes %d)", len(output))
	t.Logf("%s TTL writer subprocess pid=%d exited", backend, cmd.ProcessState.Pid())
	fake := newMaintenanceClock(now.Add(2 * time.Second))
	options := contractOptions()
	options.Clock = fake
	s, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer s.Close()
	fake.wait(t)
	created := readContractPod(t, s)
	fake.cycle(t)
	watcher := startWatch(t, s, t.Context(), contractKey, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
	event := nextWatch(t, watcher)
	require.Equal(t, watch.Deleted, event.Type)
	require.Equal(t, created.UID, event.Object.(*example.Pod).UID)
	require.True(t, storage.IsNotFound(s.Get(t.Context(), contractKey, storage.GetOptions{}, &example.Pod{})))
}

// AssertSQLDestroyWorker proves a maintenance transaction cancels and rolls back before Close returns.
func AssertSQLDestroyWorker(t *testing.T, backend string) {
	db := Open(t, backend)
	fake := newMaintenanceClock(time.Unix(1800000000, 0))
	options := contractOptions()
	options.Clock = fake
	gate := &revisionBarrier{Dialect: contractDialect(backend), entered: make(chan context.Context, 1), release: make(chan struct{})}
	s, err := sqlstorage.New(db.DB, gate, options)
	require.NoError(t, err)
	defer s.Close()
	fake.wait(t)
	createContractPod(t, s, "worker", 1)
	before := snapshotContractDB(t, db.DB)
	gate.mu.Lock()
	gate.armed = true
	gate.mu.Unlock()
	fake.Step(2 * time.Second)
	fake.tick(t)
	var workerCtx context.Context
	select {
	case workerCtx = <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance transaction did not enter")
	}
	closed := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { closed <- s.Close() }()
	}
	waitSignal(t, workerCtx.Done(), "maintenance transaction was not cancelled")
	for i := 0; i < 8; i++ {
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent Close did not finish")
		}
	}
	waitSignal(t, fake.ticker.stopped, "maintenance ticker was not stopped")
	require.Zero(t, db.DB.Stats().InUse)
	require.NoError(t, db.DB.Ping())
	require.Equal(t, before, snapshotContractDB(t, db.DB))
}

// AssertSQLTTLRollback makes expiry history fail and verifies retry from the unchanged state.
func AssertSQLTTLRollback(t *testing.T, backend string) {
	fake := newMaintenanceClock(time.Unix(1800000000, 0))
	s, db, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
	fake.wait(t)
	createContractPod(t, s, "rollback", 1)
	before := snapshotContractDB(t, db.DB)
	trigger := "CREATE TRIGGER fail_expiry BEFORE INSERT ON storage_history BEGIN SELECT RAISE(ABORT, 'injected expiry failure'); END"
	if backend == "mysql" {
		trigger = "CREATE TRIGGER fail_expiry BEFORE INSERT ON storage_history FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected expiry failure'"
	}
	_, err := db.DB.Exec(trigger)
	require.NoError(t, err)
	fake.Step(2 * time.Second)
	fake.cycle(t)
	require.Equal(t, before, snapshotContractDB(t, db.DB))
	require.Error(t, s.ReadinessCheck())
	_, err = db.DB.Exec("DROP TRIGGER fail_expiry")
	require.NoError(t, err)
	fake.cycle(t)
	require.NoError(t, s.ReadinessCheck())
	require.True(t, storage.IsNotFound(s.Get(t.Context(), contractKey, storage.GetOptions{}, &example.Pod{})))
	var deletions int
	require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_history WHERE change_type='DELETED'").Scan(&deletions))
	require.Equal(t, 1, deletions)
}
