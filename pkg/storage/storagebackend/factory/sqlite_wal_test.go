/*
 * Copyright (c) 2025 Amlogic, Inc. All rights reserved.
 *
 * This source code is subject to the terms and conditions defined in the
 * file 'LICENSE' which is part of this source code package.
 *
 * Description: Verify mandatory SQLite WAL, durability, and concurrent storage behavior.
 *
 * Author: xiaoqiang.zhou
 * Date:   2026-10-26
 */

package factory

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	clocktesting "k8s.io/utils/clock/testing"
)

func sqliteTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "apiserver-sqlite-wal-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(directory)) })
	return directory
}

func TestSQLitePoolWALDurability(t *testing.T) {
	for _, query := range []string{"", "?_journal=wal&_sync=2", "?_journal_mode=WAL&_synchronous=EXTRA"} {
		t.Run(query, func(t *testing.T) {
			path := filepath.Join(sqliteTestDirectory(t), "storage.db")
			config := storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: path + query}}
			pool, err := openSQLPool(config)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Close()) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			connections := make([]*sql.Conn, 0, 4)
			defer func() {
				for _, connection := range connections {
					require.NoError(t, connection.Close())
				}
			}()
			for i := 0; i < 4; i++ {
				connection, err := pool.Conn(ctx)
				require.NoError(t, err)
				connections = append(connections, connection)
				var journal, version string
				var synchronous int
				require.NoError(t, connection.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
				require.Equal(t, "wal", journal)
				require.NoError(t, connection.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous))
				require.GreaterOrEqual(t, synchronous, 2)
				require.NoError(t, connection.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version))
				require.Equal(t, "3.53.4", version)
			}
			_, err = connections[0].ExecContext(ctx, "CREATE TABLE durable (value TEXT NOT NULL)")
			require.NoError(t, err)
			_, err = connections[0].ExecContext(ctx, "INSERT INTO durable VALUES ('confirmed')")
			require.NoError(t, err)
			for _, connection := range connections {
				require.NoError(t, connection.Close())
			}
			connections = nil
			require.NoError(t, pool.Close())
			reopened, err := openSQLPool(config)
			require.NoError(t, err)
			defer reopened.Close()
			var actual, journal string
			require.NoError(t, reopened.QueryRowContext(ctx, "SELECT value FROM durable").Scan(&actual))
			require.Equal(t, "confirmed", actual)
			require.NoError(t, reopened.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
			require.Equal(t, "wal", journal)
		})
	}
}

func TestSQLitePoolRejectsNonWAL(t *testing.T) {
	for _, query := range []string{
		"?_journal=DELETE", "?_journal_mode=OFF", "?_journal_mode=WAL&_journal=DELETE",
		"?_journal_mode=WAL&_journal_mode=DELETE", "?_journal=wal&_journal=off",
		"?_journal_mode=", "?_synchronous=NORMAL", "?_sync=OFF",
		"?_synchronous=FULL&_sync=1", "?_sync=FULL&_sync=0",
		"?_synchronous=FULL&_synchronous=OFF", "?_synchronous=",
	} {
		t.Run(query, func(t *testing.T) {
			path := filepath.Join(sqliteTestDirectory(t), "private-secret.db")
			pool, err := openSQLPool(storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: path + query}})
			if pool != nil {
				defer pool.Close()
			}
			require.Error(t, err)
			require.Nil(t, pool)
			require.NotContains(t, err.Error(), "private-secret")
		})
	}
}

func TestSQLitePoolRejectsMemory(t *testing.T) {
	for _, dsn := range []string{":memory:", "file::memory:", "file:private-memory?mode=memory&cache=shared"} {
		t.Run(dsn, func(t *testing.T) {
			pool, err := openSQLPool(storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: dsn}})
			if pool != nil {
				defer pool.Close()
			}
			require.Error(t, err)
			require.Nil(t, pool)
		})
	}
}

func TestSQLiteRuntimeVersionPolicy(t *testing.T) {
	for _, version := range []string{"3.44.0", "3.44.6", "3.50.7", "3.51.2", "not-a-version"} {
		require.Error(t, validateSQLiteVersion(version), version)
	}
	for _, version := range []string{"3.51.3", "3.53.4", "4.0.0"} {
		require.NoError(t, validateSQLiteVersion(version), version)
	}
}

func TestSQLiteRuntimeRejectsNonWAL(t *testing.T) {
	path := filepath.Join(sqliteTestDirectory(t), "storage.db")
	pool, err := sql.Open("sqlite3", path+"?_synchronous=FULL")
	require.NoError(t, err)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.Error(t, checkSQLitePool(ctx, pool))
}

func TestSQLitePoolFailedOpenReleasesFile(t *testing.T) {
	for _, query := range []string{"?mode=ro", "?immutable=1"} {
		t.Run(query, func(t *testing.T) {
			path := filepath.Join(sqliteTestDirectory(t), "private-secret.db")
			original, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			_, err = original.Exec("CREATE TABLE existing (id INTEGER)")
			require.NoError(t, err)
			require.NoError(t, original.Close())
			pool, err := openSQLPool(storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: "file:" + path + query}})
			if pool != nil {
				defer pool.Close()
			}
			require.Error(t, err)
			require.Nil(t, pool)
			require.NotContains(t, err.Error(), "private-secret")
			if query == "?immutable=1" {
				require.ErrorContains(t, err, "effective WAL")
			}
			files, err := os.ReadDir("/proc/self/fd")
			require.NoError(t, err)
			for _, file := range files {
				target, _ := os.Readlink(filepath.Join("/proc/self/fd", file.Name()))
				require.NotEqual(t, path, target, "failed initialization retained database descriptor")
			}
		})
	}
}

func TestSQLitePoolConvertsExistingDatabase(t *testing.T) {
	path := filepath.Join(sqliteTestDirectory(t), "existing.db")
	original, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer original.Close()
	_, err = original.Exec("CREATE TABLE existing (value TEXT); INSERT INTO existing VALUES ('retained')")
	require.NoError(t, err)
	var journal string
	require.NoError(t, original.QueryRow("PRAGMA journal_mode").Scan(&journal))
	require.Equal(t, "delete", journal)
	require.NoError(t, original.Close())
	pool, err := openSQLPool(storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: path}})
	require.NoError(t, err)
	defer pool.Close()
	var actual string
	require.NoError(t, pool.QueryRow("PRAGMA journal_mode").Scan(&journal))
	require.Equal(t, "wal", journal)
	require.NoError(t, pool.QueryRow("SELECT value FROM existing").Scan(&actual))
	require.Equal(t, "retained", actual)
}

func TestSQLiteRuntimeRejectsWeakSynchronization(t *testing.T) {
	path := filepath.Join(sqliteTestDirectory(t), "storage.db")
	pool, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	require.NoError(t, err)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.ErrorContains(t, checkSQLitePool(ctx, pool), "synchronization")
}

func TestSQLiteWALWatchCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	path := filepath.Join(sqliteTestDirectory(t), "storage.db")
	config := storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: path}}
	pool, err := openSQLPool(config)
	require.NoError(t, err)
	defer pool.Close()
	headPool, err := openSQLPool(config)
	require.NoError(t, err)
	defer headPool.Close()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	fakeClock := clocktesting.NewFakeClock(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC))
	newStore := func(db *sql.DB, prefix string) *sqlstorage.Store {
		store, err := sqlite.New(db, sqlstorage.Options{
			Prefix: "/registry", ResourcePrefix: prefix,
			Codec:          serializer.NewCodecFactory(scheme).LegacyCodec(corev1.SchemeGroupVersion),
			NewFunc:        func() runtime.Object { return &corev1.ConfigMap{} },
			NewListFunc:    func() runtime.Object { return &corev1.ConfigMapList{} },
			ReverseKeyFunc: func(key string) (string, string, error) { return "", filepath.Base(key), nil },
			Clock:          fakeClock,
		})
		require.NoError(t, err)
		return store
	}
	watched := newStore(pool, "/watched")
	defer watched.Close()
	head := newStore(headPool, "/heads")
	defer head.Close()
	require.NoError(t, head.Create(ctx, "/heads/item", &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "item"}}, &corev1.ConfigMap{}, 0))
	require.NoError(t, watched.Create(ctx, "/watched/item", &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "item"}}, &corev1.ConfigMap{}, 0))
	revision, err := watched.GetCurrentResourceVersion(ctx)
	require.NoError(t, err)

	readerReady := make(chan struct{})
	allowReader := make(chan struct{})
	var readerSeen atomic.Bool
	connections := make([]*sql.Conn, 0, 4)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		connection, err := pool.Conn(ctx)
		require.NoError(t, err)
		connections = append(connections, connection)
		require.NoError(t, connection.Raw(func(raw any) error {
			raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(operation int, first, second, third string) int {
				if operation == sqlite3.SQLITE_READ && first == "storage_history" && readerSeen.CompareAndSwap(false, true) {
					close(readerReady)
					select {
					case <-allowReader:
					case <-ctx.Done():
					}
				}
				return sqlite3.SQLITE_OK
			})
			return nil
		}))
	}
	for _, connection := range connections {
		require.NoError(t, connection.Close())
	}
	watcher, err := watched.Watch(ctx, "/watched/", storage.ListOptions{
		ResourceVersion: strconv.FormatUint(revision-1, 10), Recursive: true,
		Predicate: storage.SelectionPredicate{Label: labels.Everything(), Field: fields.Everything()},
	})
	require.NoError(t, err)
	defer watcher.Stop()
	// Cancellation releases the barrier before Stop/Close join workers on failure.
	defer cancel()
	select {
	case <-readerReady:
	case <-ctx.Done():
		t.Fatal("watch did not reach history query after acquiring its read snapshot")
	}
	compactDone := make(chan error, 1)
	go func() { _, err := watched.Compact(ctx, revision-1); compactDone <- err }()
	defer func() {
		cancel()
		<-compactDone
	}()
	select {
	case err := <-compactDone:
		// Keep completion available for the cleanup join.
		compactDone <- err
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("compaction could not commit while watch held its read snapshot")
	}
	require.NoError(t, head.Get(ctx, "/heads/item", storage.GetOptions{}, &corev1.ConfigMap{}))
	close(allowReader)
	select {
	case event := <-watcher.ResultChan():
		require.Equal(t, watch.Added, event.Type)
		object, ok := event.Object.(*corev1.ConfigMap)
		require.True(t, ok)
		require.Equal(t, "item", object.Name)
	case <-ctx.Done():
		t.Fatal("watch did not deliver event after compaction")
	}
}
