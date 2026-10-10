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
	"bytes"
	"context"
	"crypto/aes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/apis/example/install"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	mysqlstorage "k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	"k8s.io/apiserver/pkg/storage/value"
	encryptaes "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	clocktesting "k8s.io/utils/clock/testing"
)

const contractKey = "/pods/ns/name"

func contractOptions() sqlstorage.Options {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	return sqlstorage.Options{ResourcePrefix: "/pods", Codec: serializer.NewCodecFactory(scheme).LegacyCodec(examplev1.SchemeGroupVersion), NewFunc: func() runtime.Object { return &example.Pod{} }, NewListFunc: func() runtime.Object { return &example.PodList{} }, ReverseKeyFunc: func(key string) (string, string, error) {
		parts := strings.Split(key, "/")
		if len(parts) != 4 || parts[1] != "pods" {
			return "", "", fmt.Errorf("invalid resource key")
		}
		return parts[3], parts[2], nil
	}}
}

func openContract(t *testing.T, backend string, configure func(*sqlstorage.Options)) (*sqlstorage.Store, *Database, sqlstorage.Options) {
	t.Helper()
	database := Open(t, backend)
	options := contractOptions()
	if configure != nil {
		configure(&options)
	}
	store, err := sqlstorage.New(database.DB, contractDialect(backend), options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, database, options
}
func contractDialect(backend string) sqlstorage.Dialect {
	if backend == "mysql" {
		return mysqlstorage.NewDialect()
	}
	return sqlite.NewDialect()
}
func contractPod(value string) *example.Pod {
	return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "ns", UID: types.UID(value), Labels: map[string]string{"value": value}}}
}
func createContractPod(t *testing.T, store *sqlstorage.Store, value string, ttl uint64) *example.Pod {
	t.Helper()
	out := &example.Pod{}
	require.NoError(t, store.Create(context.Background(), contractKey, contractPod(value), out, ttl))
	return out
}
func readContractPod(t *testing.T, store *sqlstorage.Store) *example.Pod {
	t.Helper()
	out := &example.Pod{}
	require.NoError(t, store.Get(context.Background(), contractKey, storage.GetOptions{}, out))
	return out
}

// AssertSQLCreateConflict catches accidental overwrite and failed-operation caller mutation.
func AssertSQLCreateConflict(t *testing.T, backend string) {
	t.Run("digest-collision-is-not-key-exists", func(t *testing.T) {
		store, db, _ := openContract(t, backend, nil)
		createContractPod(t, store, "original", 0)
		// Corrupt only the raw key to exercise a digest collision without weakening SHA-256.
		_, err := db.DB.Exec("UPDATE storage_objects SET storage_key=? WHERE storage_key=?", []byte("/pods/ns/different"), []byte(contractKey))
		require.NoError(t, err)
		before := snapshotContractDB(t, db.DB)
		err = store.Create(context.Background(), contractKey, contractPod("new"), nil, 0)
		require.Error(t, err)
		require.False(t, storage.IsExist(err) || storage.IsConflict(err))
		require.Equal(t, before, snapshotContractDB(t, db.DB))
	})
	t.Run("reopen-and-schema-version", func(t *testing.T) {
		store, db, options := openContract(t, backend, nil)
		created := createContractPod(t, store, "original", 0)
		require.NoError(t, store.Close())
		reopened, err := sqlstorage.New(db.DB, contractDialect(backend), options)
		require.NoError(t, err)
		defer reopened.Close()
		require.Equal(t, created, readContractPod(t, reopened))
		_, err = db.DB.Exec("UPDATE storage_meta SET schema_version=99 WHERE id=1")
		require.NoError(t, err)
		before := snapshotContractDB(t, db.DB)
		_, err = sqlstorage.New(db.DB, contractDialect(backend), options)
		require.ErrorContains(t, err, "schema version")
		require.Equal(t, before, snapshotContractDB(t, db.DB))
	})

	store, db, options := openContract(t, backend, nil)
	original := createContractPod(t, store, "original", 0)
	input := contractPod("replacement")
	input.SelfLink = "preserve"
	before := input.DeepCopy()
	out := contractPod("sentinel")
	outBefore := out.DeepCopy()
	err := store.Create(context.Background(), contractKey, input, out, 0)
	require.True(t, storage.IsExist(err), "expected KeyExists, got %v", err)
	require.Equal(t, before, input)
	require.Equal(t, outBefore, out)
	require.Equal(t, original, readContractPod(t, store))
	var revision uint64
	require.NoError(t, db.DB.QueryRow("SELECT revision FROM storage_meta WHERE id=1").Scan(&revision))
	require.Greater(t, revision, uint64(0))
	for _, key := range []string{"/pods/ns/Name", "/pods/ns/name ", "/pods/ns/na\x00me", "/pods/ns/" + strings.Repeat("long", 2048) + "a", "/pods/ns/" + strings.Repeat("long", 2048) + "b"} {
		require.NoError(t, store.Create(context.Background(), key, contractPod(key), nil, 0))
		got := &example.Pod{}
		require.NoError(t, store.Get(context.Background(), key, storage.GetOptions{}, got))
		require.Equal(t, types.UID(key), got.UID)
	}
	options.ResourcePrefix = "/other"
	other, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer other.Close()
	require.NoError(t, other.Create(context.Background(), "/other/ns/name", contractPod("other"), nil, 0))
	require.Equal(t, original, readContractPod(t, store))
	for _, key := range []string{"", "/", "/pods/../name", "/pods/./name", "/other/ns/name"} {
		require.Error(t, store.Create(context.Background(), key, contractPod("invalid"), nil, 0))
	}
	missing := contractPod("sentinel")
	require.True(t, storage.IsNotFound(store.Get(context.Background(), "/pods/ns/missing", storage.GetOptions{}, missing)))
	require.Equal(t, contractPod("sentinel"), missing)
	require.NoError(t, store.Get(context.Background(), "/pods/ns/missing", storage.GetOptions{IgnoreNotFound: true}, missing))
	require.Equal(t, &example.Pod{}, missing)
	require.True(t, storage.IsTooLargeResourceVersion(store.Get(context.Background(), contractKey, storage.GetOptions{ResourceVersion: "999999999"}, &example.Pod{})))
}

// AssertSQLConcurrentUpdateWinner detects lock-held callbacks and false conflict accounting.
func AssertSQLConcurrentUpdateWinner(t *testing.T, backend string) {
	t.Run("create-on-update", func(t *testing.T) {
		store, _, _ := openContract(t, backend, nil)
		out := contractPod("sentinel")
		callback := func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
			require.Equal(t, &example.Pod{}, obj)
			require.Zero(t, meta.ResourceVersion)
			return contractPod("created"), nil, nil
		}
		require.True(t, storage.IsNotFound(store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, callback, nil)))
		require.Equal(t, contractPod("sentinel"), out)
		require.NoError(t, store.GuaranteedUpdate(context.Background(), contractKey, out, true, nil, callback, nil))
		require.Equal(t, out, readContractPod(t, store))
	})

	store, db, options := openContract(t, backend, nil)
	old := createContractPod(t, store, "original", 0)
	second, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	type result struct {
		value string
		err   error
		out   *example.Pod
	}
	results := make(chan result, 2)
	for index, s := range []*sqlstorage.Store{store, second} {
		go func(index int, s *sqlstorage.Store) {
			value := fmt.Sprintf("writer-%d", index)
			out := contractPod("sentinel")
			err := s.GuaranteedUpdate(ctx, contractKey, out, false, &storage.Preconditions{ResourceVersion: &old.ResourceVersion}, func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
				arrived <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
				pod := obj.(*example.Pod)
				pod.Labels["value"] = value
				return pod, nil, nil
			}, nil)
			results <- result{value, err, out}
		}(index, s)
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("callbacks did not both enter outside the database write lock")
		}
	}
	close(release)
	successfulWriters, conflicts := 0, 0
	winnerValue := ""
	for range 2 {
		r := <-results
		if r.err == nil {
			successfulWriters++
			winnerValue = r.value
		} else {
			require.True(t, storage.IsConflict(r.err) || (storage.IsInvalidObj(r.err) && strings.Contains(r.err.Error(), "Precondition failed: ResourceVersion")), "unexpected database/other failure: %v", r.err)
			conflicts++
			require.Equal(t, contractPod("sentinel"), r.out)
		}
	}
	require.Equal(t, 1, successfulWriters)
	require.Equal(t, 1, conflicts)
	stored := readContractPod(t, store)
	require.Equal(t, winnerValue, stored.Labels["value"])
	beforeRV, err := store.Versioner().ObjectResourceVersion(old)
	require.NoError(t, err)
	afterRV, err := store.Versioner().ObjectResourceVersion(stored)
	require.NoError(t, err)
	require.Greater(t, afterRV, beforeRV)
	// A callback may run another write on the same Store; CAS then retries with fresh state.
	calls := 0
	require.NoError(t, store.GuaranteedUpdate(ctx, contractKey, &example.Pod{}, false, nil, func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
		calls++
		pod := obj.(*example.Pod)
		if calls == 1 {
			err := store.GuaranteedUpdate(ctx, contractKey, &example.Pod{}, false, nil, storage.SimpleUpdate(func(other runtime.Object) (runtime.Object, error) {
				other.(*example.Pod).Labels["nested"] = "yes"
				return other, nil
			}), nil)
			if err != nil {
				return nil, nil, err
			}
		} else {
			require.Equal(t, "yes", pod.Labels["nested"])
			rv, _ := store.Versioner().ObjectResourceVersion(pod)
			require.Equal(t, rv, meta.ResourceVersion)
		}
		pod.Labels["outer"] = "yes"
		return pod, nil, nil
	}, nil))
	require.Equal(t, 2, calls)
}

// AssertSQLDeleteRecreatePreconditions rejects stale identities after key reuse.
func AssertSQLDeleteRecreatePreconditions(t *testing.T, backend string) {
	store, _, _ := openContract(t, backend, nil)
	old := createContractPod(t, store, "old", 0)
	deleted := &example.Pod{}
	require.NoError(t, store.Delete(context.Background(), contractKey, deleted, &storage.Preconditions{UID: &old.UID}, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}))
	require.Equal(t, old.UID, deleted.UID)
	require.NotEqual(t, old.ResourceVersion, deleted.ResourceVersion)
	current := createContractPod(t, store, "new", 0)
	for _, pre := range []*storage.Preconditions{{UID: &old.UID}, {ResourceVersion: &old.ResourceVersion}} {
		out := contractPod("sentinel")
		err := store.Delete(context.Background(), contractKey, out, pre, storage.ValidateAllObjectFunc, old, storage.DeleteOptions{})
		require.True(t, storage.IsInvalidObj(err), "%v", err)
		require.Equal(t, contractPod("sentinel"), out)
		require.Equal(t, current, readContractPod(t, store))
	}
	sentinel := errors.New("validation denied")
	require.ErrorIs(t, store.Delete(context.Background(), contractKey, &example.Pod{}, nil, func(context.Context, runtime.Object) error { return sentinel }, nil, storage.DeleteOptions{}), sentinel)
}

func snapshotContractDB(t *testing.T, db *sql.DB) string {
	t.Helper()
	var result strings.Builder
	for _, table := range []string{"storage_meta", "storage_objects", "storage_history"} {
		rows, err := db.Query("SELECT * FROM " + table + " ORDER BY 1")
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			require.NoError(t, rows.Scan(pointers...))
			fmt.Fprintf(&result, "%s:%#v\n", table, values)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return result.String()
}

// AssertSQLMutationRollback uses a database trigger to fail history writes after other writes.
func AssertSQLMutationRollback(t *testing.T, backend string) {
	t.Run("close-cancels-pending-callback-without-owning-pool", func(t *testing.T) {
		store, db, _ := openContract(t, backend, nil)
		createContractPod(t, store, "original", 0)
		before := snapshotContractDB(t, db.DB)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		done := make(chan error, 1)
		go func() {
			done <- store.GuaranteedUpdate(context.Background(), contractKey, &example.Pod{}, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
				close(entered)
				<-release
				obj.(*example.Pod).Labels["closed"] = "yes"
				return obj, nil, nil
			}, nil)
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("callback did not enter")
		}
		closing := make(chan error, 1)
		watcher := startWatch(t, store, context.Background(), contractKey, storage.ListOptions{ResourceVersion: "1", Predicate: storage.Everything})
		go func() { closing <- store.Close() }()
		watchClosed := make(chan struct{})
		go func() {
			for range watcher.ResultChan() {
			}
			close(watchClosed)
		}()
		select {
		case <-watchClosed:
		case <-time.After(10 * time.Second):
			t.Fatal("watch did not acknowledge Store cancellation")
		}
		unblock()
		select {
		case err := <-closing:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not drain callback")
		}
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled update did not return")
		}
		require.NoError(t, store.Close())
		require.NoError(t, db.DB.Ping())
		require.Equal(t, before, snapshotContractDB(t, db.DB))
	})

	store, db, _ := openContract(t, backend, nil)
	createContractPod(t, store, "original", 0)
	before := snapshotContractDB(t, db.DB)
	trigger := "CREATE TRIGGER fail_history BEFORE INSERT ON storage_history BEGIN SELECT RAISE(ABORT, 'injected history failure'); END"
	if backend == "mysql" {
		trigger = "CREATE TRIGGER fail_history BEFORE INSERT ON storage_history FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected history failure'"
	}
	_, err := db.DB.Exec(trigger)
	require.NoError(t, err)
	input := contractPod("candidate")
	input.SelfLink = "preserve"
	inputBefore := input.DeepCopy()
	for _, operation := range []string{"create", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			out := contractPod("sentinel")
			var err error
			switch operation {
			case "create":
				err = store.Create(context.Background(), "/pods/ns/other", input, out, 0)
			case "update":
				err = store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, func(runtime.Object, storage.ResponseMeta) (runtime.Object, *uint64, error) { return input, nil, nil }, nil)
			case "delete":
				err = store.Delete(context.Background(), contractKey, out, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
			}
			require.Error(t, err)
			require.False(t, storage.IsConflict(err) || storage.IsInvalidObj(err) || storage.IsExist(err))
			require.Equal(t, inputBefore, input)
			require.Equal(t, contractPod("sentinel"), out)
			require.Equal(t, before, snapshotContractDB(t, db.DB))
		})
	}
	_, err = db.DB.Exec("DROP TRIGGER fail_history")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, store.Create(ctx, "/pods/ns/cancelled", input, &example.Pod{}, 0))
	require.Equal(t, before, snapshotContractDB(t, db.DB))
	sentinel := errors.New("callback failure")
	require.ErrorIs(t, store.GuaranteedUpdate(context.Background(), contractKey, &example.Pod{}, false, nil, func(runtime.Object, storage.ResponseMeta) (runtime.Object, *uint64, error) { return nil, nil, sentinel }, nil), sentinel)
	require.Equal(t, before, snapshotContractDB(t, db.DB))
}

type contractTransformer struct {
	value.Transformer
	failRead  atomic.Bool
	failWrite atomic.Bool
	stale     atomic.Bool
	wrongAAD  atomic.Bool
}

var transformFailure = errors.New("injected transform failure")

func (tr *contractTransformer) TransformFromStorage(ctx context.Context, data []byte, aad value.Context) ([]byte, bool, error) {
	if string(aad.AuthenticatedData()) != contractKey {
		tr.wrongAAD.Store(true)
	}
	if tr.failRead.Load() {
		return nil, false, transformFailure
	}
	plain, stale, err := tr.Transformer.TransformFromStorage(ctx, data, aad)
	return plain, stale || tr.stale.Load(), err
}
func (tr *contractTransformer) TransformToStorage(ctx context.Context, data []byte, aad value.Context) ([]byte, error) {
	if string(aad.AuthenticatedData()) != contractKey {
		tr.wrongAAD.Store(true)
	}
	if tr.failWrite.Load() {
		return nil, transformFailure
	}
	return tr.Transformer.TransformToStorage(ctx, data, aad)
}
func newContractTransformer(t *testing.T) *contractTransformer {
	t.Helper()
	block, err := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	tr, err := encryptaes.NewGCMTransformer(block)
	require.NoError(t, err)
	return &contractTransformer{Transformer: tr}
}

// AssertSQLCodecTransformerRoundTrip proves real encryption, AAD, TTL, stale rewrites and no-op behavior.
func AssertSQLCodecTransformerRoundTrip(t *testing.T, backend string) {
	tr := newContractTransformer(t)
	fakeClock := clocktesting.NewFakeClock(time.Unix(1800000000, 0))
	store, db, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Transformer = tr; o.Clock = fakeClock })
	old := createContractPod(t, store, "secret-value", 60)
	require.Equal(t, old, readContractPod(t, store))
	var ciphertext []byte
	require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte(contractKey)).Scan(&ciphertext))
	require.NotContains(t, string(ciphertext), "secret-value")
	require.False(t, tr.wrongAAD.Load())
	before := snapshotContractDB(t, db.DB)
	fakeClock.Step(10 * time.Second)
	out := &example.Pod{}
	require.NoError(t, store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
		require.Equal(t, int64(50), meta.TTL)
		return obj, nil, nil
	}, nil))
	require.Equal(t, old, out)
	require.Equal(t, before, snapshotContractDB(t, db.DB))
	tr.stale.Store(true)
	require.NoError(t, store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) { return obj, nil }), nil))
	require.NotEqual(t, old.ResourceVersion, out.ResourceVersion)
	tr.stale.Store(false)
	var expiry int64
	require.NoError(t, db.DB.QueryRow("SELECT expires_at FROM storage_objects WHERE storage_key=?", []byte(contractKey)).Scan(&expiry))
	require.Equal(t, time.Unix(1800000060, 0).UnixNano(), expiry)
	// Explicit zero TTL removes expiry, and therefore must not be treated as a content no-op.
	require.NoError(t, store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, func(obj runtime.Object, meta storage.ResponseMeta) (runtime.Object, *uint64, error) {
		zero := uint64(0)
		return obj, &zero, nil
	}, nil))
	var noExpiry sql.NullInt64
	require.NoError(t, db.DB.QueryRow("SELECT expires_at FROM storage_objects WHERE storage_key=?", []byte(contractKey)).Scan(&noExpiry))
	require.False(t, noExpiry.Valid)
	before = snapshotContractDB(t, db.DB)
	tr.failWrite.Store(true)
	input := contractPod("failed")
	input.SelfLink = "preserve"
	inputBefore := input.DeepCopy()
	err := store.GuaranteedUpdate(context.Background(), contractKey, out, false, nil, func(runtime.Object, storage.ResponseMeta) (runtime.Object, *uint64, error) { return input, nil, nil }, nil)
	require.ErrorIs(t, err, transformFailure)
	require.Equal(t, inputBefore, input)
	require.Equal(t, before, snapshotContractDB(t, db.DB))
	tr.failWrite.Store(false)
	tr.failRead.Store(true)
	require.ErrorIs(t, store.Get(context.Background(), contractKey, storage.GetOptions{}, out), transformFailure)
	tr.failRead.Store(false)
	// Authentication must bind the exact storage key: ciphertext moved to another key is unreadable.
	movedDigest := sha256.Sum256([]byte("/pods/ns/moved"))
	_, err = db.DB.Exec("UPDATE storage_objects SET key_digest=?,storage_key=? WHERE storage_key=?", movedDigest[:], []byte("/pods/ns/moved"), []byte(contractKey))
	require.NoError(t, err)
	err = store.Get(context.Background(), "/pods/ns/moved", storage.GetOptions{}, out)
	require.Error(t, err)
	require.False(t, storage.IsNotFound(err))
}

// AssertSQLUnsafeDelete mirrors target etcd malformed-delete cases without requiring the future full interface.
func AssertSQLUnsafeDelete(t *testing.T, backend string) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)
	for _, mode := range []string{"transform", "decode", "decodable", "repaired", "precondition", "reverse-failure", "history-transform-failure", "history-encode-failure"} {
		t.Run(mode, func(t *testing.T) {
			tr := newContractTransformer(t)
			encoder := &contractCodec{}
			store, db, options := openContract(t, backend, func(o *sqlstorage.Options) {
				o.Transformer = tr
				encoder.Codec = o.Codec
				o.Codec = encoder
				if mode == "reverse-failure" {
					o.ReverseKeyFunc = func(string) (string, string, error) { return "", "", errors.New("reverse failed") }
				}
			})
			original := createContractPod(t, store, "original", 0)
			if mode == "decode" {
				data, err := tr.TransformToStorage(context.Background(), []byte("invalid json"), value.DefaultContext(contractKey))
				require.NoError(t, err)
				_, err = db.DB.Exec("UPDATE storage_objects SET object=? WHERE storage_key=?", data, []byte(contractKey))
				require.NoError(t, err)
			} else if mode != "decodable" {
				tr.failRead.Store(true)
			}
			before := snapshotContractDB(t, db.DB)
			out := contractPod("sentinel")
			validateCalls := 0
			validate := func(ctx context.Context, obj runtime.Object) error {
				validateCalls++
				require.Nil(t, obj)
				if mode == "repaired" {
					tr.failRead.Store(false)
					return store.GuaranteedUpdate(ctx, contractKey, &example.Pod{}, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
						obj.(*example.Pod).Labels["repaired"] = "yes"
						return obj, nil
					}), nil)
				}
				return nil
			}
			var pre *storage.Preconditions
			if mode == "precondition" {
				pre = &storage.Preconditions{UID: &original.UID}
			}
			if mode == "history-encode-failure" {
				encoder.failEncode.Store(true)
			}
			if mode == "history-transform-failure" {
				tr.failWrite.Store(true)
			}
			err := store.Delete(context.Background(), contractKey, out, pre, validate, original, storage.DeleteOptions{ExpectTransformOrDecodeError: true})
			require.Equal(t, contractPod("sentinel"), out)
			switch mode {
			case "decodable", "repaired":
				require.True(t, storage.IsInvalidObj(err), "%v", err)
			case "precondition", "reverse-failure", "history-transform-failure", "history-encode-failure":
				require.Error(t, err)
				require.Equal(t, before, snapshotContractDB(t, db.DB))
			default:
				require.NoError(t, err)
				require.Equal(t, 1, validateCalls)
				require.True(t, storage.IsNotFound(store.Get(context.Background(), contractKey, storage.GetOptions{}, &example.Pod{})))
				tr.failRead.Store(false)
				var previous []byte
				require.NoError(t, db.DB.QueryRow("SELECT previous_object FROM storage_history WHERE change_type='DELETED'").Scan(&previous))
				plain, _, err := tr.TransformFromStorage(context.Background(), previous, value.DefaultContext(contractKey))
				require.NoError(t, err)
				identity := &example.Pod{}
				require.NoError(t, runtime.DecodeInto(options.Codec, plain, identity))
				require.Equal(t, "name", identity.Name)
				require.Equal(t, "ns", identity.Namespace)
				require.Empty(t, identity.UID)
			}
		})
	}
	t.Run("feature-disabled", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, false)
		tr := newContractTransformer(t)
		store, _, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Transformer = tr })
		createContractPod(t, store, "original", 0)
		tr.failRead.Store(true)
		err := store.Delete(context.Background(), contractKey, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{ExpectTransformOrDecodeError: true})
		require.ErrorIs(t, err, transformFailure)
	})
}

// contractCodec injects an encode failure while retaining the real Kubernetes codec.
type contractCodec struct {
	runtime.Codec
	failEncode atomic.Bool
}

func (c *contractCodec) Encode(obj runtime.Object, w io.Writer) error {
	if c.failEncode.Load() {
		return errors.New("injected encode failure")
	}
	return c.Codec.Encode(obj, w)
}
