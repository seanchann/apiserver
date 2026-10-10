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

//go:build integration

package storage_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/apis/example/install"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	mysqlstorage "k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	"k8s.io/apiserver/test/integration/storage/framework"
)

// The writer deliberately holds a real transaction after all three persisted
// records are changed. The parent sees an explicit staging barrier, then queries
// and requests watch progress before allowing commit or rollback.
func TestBackendMultiProcessWatch(t *testing.T) {
	if backend := os.Getenv("TASK9_WRITER_BACKEND"); backend != "" {
		runStagedWriter(t, backend)
		return
	}
	for _, backend := range []string{"sqlite", "mysql"} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%t", backend, commit), func(t *testing.T) {
				s, db, _ := newBackend(t, backend)
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				key := "/pods/ns/staged"
				created := &example.Pod{}
				require.NoError(t, s.Create(ctx, key, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "staged", Namespace: "ns", Labels: map[string]string{"state": "before"}}}, created, 0))
				w, err := s.Watch(ctx, key, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
				require.NoError(t, err)
				defer w.Stop()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackendMultiProcessWatch$", "-test.count=1", "-test.timeout=25s")
				cmd.Env = append(os.Environ(), "TASK9_WRITER_BACKEND="+backend, "TASK9_WRITER_DSN="+db.DSN)
				output, err := cmd.StdoutPipe()
				require.NoError(t, err)
				input, err := cmd.StdinPipe()
				require.NoError(t, err)
				var diagnostics bytes.Buffer
				cmd.Stderr = &diagnostics
				require.NoError(t, cmd.Start())
				defer func() {
					input.Close()
					if cmd.ProcessState == nil {
						cmd.Process.Kill()
						cmd.Wait()
					}
				}()
				messages := make(chan string, 8)
				go func() {
					defer close(messages)
					scanner := bufio.NewScanner(output)
					for scanner.Scan() {
						messages <- scanner.Text()
					}
				}()
				select {
				case message := <-messages:
					require.Equal(t, "STAGED", message)
				case <-ctx.Done():
					t.Fatal("writer did not stage its transaction")
				}
				old := &example.Pod{}
				require.NoError(t, s.Get(ctx, key, storage.GetOptions{}, old))
				require.Equal(t, created, old)
				var histories int
				require.NoError(t, db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM storage_history").Scan(&histories))
				require.Equal(t, 1, histories)
				require.NoError(t, s.RequestWatchProgress(ctx))
				event := receiveEvent(t, w)
				require.Equal(t, watch.Bookmark, event.Type)
				require.Equal(t, created.ResourceVersion, event.Object.(*example.Pod).ResourceVersion)
				command := "ROLLBACK"
				if commit {
					command = "COMMIT"
				}
				_, err = fmt.Fprintln(input, command)
				require.NoError(t, err)
				input.Close()
				require.NoError(t, cmd.Wait(), "writer failed; diagnostic bytes=%d", diagnostics.Len())
				if commit {
					event = receiveEvent(t, w)
					require.Equal(t, watch.Modified, event.Type)
					require.Equal(t, "after", event.Object.(*example.Pod).Labels["state"])
					require.NotEqual(t, created.ResourceVersion, event.Object.(*example.Pod).ResourceVersion)
				} else {
					require.NoError(t, s.RequestWatchProgress(ctx))
					event = receiveEvent(t, w)
					require.Equal(t, watch.Bookmark, event.Type)
					require.Equal(t, created.ResourceVersion, event.Object.(*example.Pod).ResourceVersion)
				}
				t.Logf("%s writer PID %d: uncommitted history invisible; %s boundary verified", backend, cmd.ProcessState.Pid(), command)
			})
		}
	}
}
func receiveEvent(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()
	select {
	case e, ok := <-w.ResultChan():
		require.True(t, ok)
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("watch event deadline exceeded")
		return watch.Event{}
	}
}
func runStagedWriter(t *testing.T, backend string) {
	driver := "sqlite3"
	var dialect sqlstorage.Dialect = sqlite.NewDialect()
	if backend == "mysql" {
		driver = "mysql"
		dialect = mysqlstorage.NewDialect()
	}
	db, err := sql.Open(driver, os.Getenv("TASK9_WRITER_DSN"))
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	revision, err := dialect.LockRevision(ctx, tx)
	require.NoError(t, err)
	revision++
	key := "/pods/ns/staged"
	digest := sha256.Sum256([]byte(key))
	var previous []byte
	var oldRevision uint64
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT object,revision FROM storage_objects WHERE storage_key=?", []byte(key)).Scan(&previous, &oldRevision))
	scheme := runtime.NewScheme()
	install.Install(scheme)
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(examplev1.SchemeGroupVersion)
	pod := &example.Pod{}
	require.NoError(t, runtime.DecodeInto(codec, previous, pod))
	pod.Labels["state"] = "after"
	pod.ResourceVersion = ""
	encoded, err := runtime.Encode(codec, pod)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE storage_objects SET revision=?,object=? WHERE storage_key=?", revision, encoded, []byte(key))
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "UPDATE storage_meta SET revision=? WHERE id=1", revision)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO storage_history(revision,key_digest,storage_key,resource_prefix,change_type,previous_object,object,previous_revision,object_revision,previous_expires_at,expires_at,committed_at) VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,?)", revision, digest[:], []byte(key), []byte("/pods"), "MODIFIED", previous, encoded, oldRevision, revision, time.Now().UnixNano())
	require.NoError(t, err)
	fmt.Fprintln(os.Stdout, "STAGED")
	scanner := bufio.NewScanner(os.Stdin)
	require.True(t, scanner.Scan())
	switch scanner.Text() {
	case "COMMIT":
		require.NoError(t, tx.Commit())
	case "ROLLBACK":
		require.NoError(t, tx.Rollback())
	default:
		t.Fatal("unknown writer transaction command")
	}
}

func TestBackendShutdown(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			t.Run("cancel-stop-close-database-fault", func(t *testing.T) { framework.AssertSQLWatchStopAndCancel(t, backend) })
			t.Run("slow-consumer", func(t *testing.T) { framework.AssertSQLSlowWatcher(t, backend) })
			t.Run("active-operation-drain", func(t *testing.T) { framework.AssertSQLDestroyDuringActivity(t, backend) })
			t.Run("blocked-query-drain", func(t *testing.T) { framework.AssertSQLDestroyBlockedQuery(t, backend) })
			t.Run("worker-drain", func(t *testing.T) { framework.AssertSQLDestroyWorker(t, backend) })
			t.Run("closed-store-rejects-watch", func(t *testing.T) {
				s, db, _ := newBackend(t, backend)
				require.NoError(t, s.Close())
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				_, err := s.Watch(ctx, "/pods", storage.ListOptions{ResourceVersion: "0", Recursive: true, Predicate: storage.Everything})
				require.Error(t, err)
				require.Equal(t, 0, db.DB.Stats().InUse)
			})
		})
	}
}

// MySQL uses only explicitly supplied control scripts for an owned test service.
// SQLite is embedded, so its equivalent disconnection closes and reopens the
// native database pool while preserving the file and committed history.
func TestBackendWatchDatabaseRestart(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			s, db, opts := newBackend(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			key := "/pods/ns/restart"
			created := &example.Pod{}
			require.NoError(t, s.Create(ctx, key, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: "ns"}}, created, 0))
			w, err := s.Watch(ctx, key, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
			require.NoError(t, err)
			defer w.Stop()
			control := func(name string) {
				t.Helper()
				directory := os.Getenv("SQL_STORAGE_TEST_MYSQL_CONTROL_DIR")
				require.NotEmpty(t, directory, "database restart requires explicitly owned MySQL service control scripts")
				controlContext, controlCancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer controlCancel()
				cmd := exec.CommandContext(controlContext, filepath.Join(directory, name))
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "private database control failed; diagnostic bytes=%d", len(output))
			}
			if backend == "mysql" {
				control("stop.sh")
				defer func() {
					if db.DB.PingContext(context.Background()) != nil {
						control("restart.sh")
					}
				}()
			} else {
				require.NoError(t, db.DB.Close())
			}
			require.Error(t, s.ReadinessCheck())
			require.Error(t, s.Get(ctx, key, storage.GetOptions{}, &example.Pod{}))
			event := receiveEvent(t, w)
			require.Equal(t, watch.Error, event.Type)
			select {
			case _, ok := <-w.ResultChan():
				require.False(t, ok)
			case <-time.After(10 * time.Second):
				t.Fatal("database fault did not close watch")
			}
			w.Stop()
			require.NoError(t, s.Close())
			require.Zero(t, db.DB.Stats().InUse)
			var restarted *sqlstorage.Store
			if backend == "mysql" {
				control("restart.sh")
				require.Eventually(t, func() bool { return db.DB.PingContext(ctx) == nil }, 20*time.Second, 50*time.Millisecond)
				restarted, err = sqlstorage.New(db.DB, mysqlstorage.NewDialect(), opts)
			} else {
				reopened, openErr := sql.Open("sqlite3", db.DSN)
				require.NoError(t, openErr)
				defer reopened.Close()
				restarted, err = sqlstorage.New(reopened, sqlite.NewDialect(), opts)
			}
			require.NoError(t, err)
			defer restarted.Close()
			require.NoError(t, restarted.ReadinessCheck())
			stored := &example.Pod{}
			require.NoError(t, restarted.Get(ctx, key, storage.GetOptions{}, stored))
			require.Equal(t, created, stored)
			resumed, err := restarted.Watch(ctx, key, storage.ListOptions{ResourceVersion: created.ResourceVersion, Predicate: storage.Everything})
			require.NoError(t, err)
			defer resumed.Stop()
			updated := &example.Pod{}
			require.NoError(t, restarted.GuaranteedUpdate(ctx, key, updated, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
				pod := obj.(*example.Pod)
				pod.Labels = map[string]string{"recovered": "true"}
				return pod, nil
			}), nil))
			event = receiveEvent(t, resumed)
			require.Equal(t, watch.Modified, event.Type)
			require.Equal(t, updated, event.Object)
			t.Logf("%s native database interruption returned errors, closed watch, drained handles and recovered committed revision", backend)
		})
	}
}
