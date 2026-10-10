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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/apis/example/install"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func legacyFixture(t *testing.T, backend string) (*Database, []sqlstorage.LegacyMapping) {
	t.Helper()
	db := Open(t, backend)
	ddl := "CREATE TABLE keyval (key TEXT, revision interger, obj TEXT, PRIMARY KEY(key))"
	table := "keyval"
	if backend == "mysql" {
		table = "pods"
		ddl = "CREATE TABLE pods (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, name varchar(255) DEFAULT NULL, namespace varchar(255) DEFAULT NULL, revision bigint DEFAULT NULL, obj json NOT NULL, UNIQUE KEY resource_idx(name,namespace)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	}
	_, err := db.DB.Exec(ddl)
	require.NoError(t, err)
	for _, name := range []string{"first", "second"} {
		obj := contractPod(name)
		obj.Name = name
		obj.ResourceVersion = "1"
		raw, err := runtime.Encode(contractOptions().Codec, obj)
		require.NoError(t, err)
		if backend == "mysql" {
			_, err = db.DB.Exec("INSERT INTO pods(name,namespace,revision,obj) VALUES(?,?,?,?)", name, "ns", 1, raw)
		} else {
			_, err = db.DB.Exec("INSERT INTO keyval(key,revision,obj) VALUES(?,?,?)", "/registry/pods/ns/"+name, 1, raw)
		}
		require.NoError(t, err)
	}
	return db, []sqlstorage.LegacyMapping{{SourceTable: table, ResourcePrefix: "/registry/pods", NamespaceScoped: true, Codec: contractOptions().Codec, Transformer: identity.NewEncryptCheckTransformer()}}
}

// AssertLegacyMigrationRestartAndRollback proves rollback and actual child-process interruption.
func AssertLegacyMigrationRestartAndRollback(t *testing.T, backend string) {
	if os.Getenv("SQL_LEGACY_CHILD") == "1" {
		legacyChild(t, backend)
		return
	}
	for _, failure := range []string{"copy-error", "process-exit", "ddl-exit"} {
		t.Run(failure, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			sourceBefore := legacySourceDigest(t, db.DB, backend)
			beforeUIDs, beforeContentDigest := legacyExpected(t, db.DB, backend)
			options := contractOptions()
			options.Prefix = "/registry"
			_, err := sqlstorage.New(db.DB, contractDialect(backend), options)
			require.ErrorContains(t, err, "migration")
			original := mappings[0].Transformer
			if failure == "copy-error" {
				mappings[0].Transformer = &legacyFailureTransformer{Transformer: original}
				_, err = sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
				require.ErrorContains(t, err, "transformation")
			} else {
				cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyMigrationRestartAndRollback$")
				cmd.Env = append(os.Environ(), "SQL_LEGACY_CHILD=1", "SQL_LEGACY_STAGE="+failure, "SQL_LEGACY_DSN="+db.DSN)
				output, err := cmd.CombinedOutput()
				require.Error(t, err)
				require.Contains(t, string(output), "migration-copy-barrier")
				require.Equal(t, 23, cmd.ProcessState.ExitCode())
			}
			mappings[0].Transformer = original
			require.Equal(t, sourceBefore, legacySourceDigest(t, db.DB, backend))
			var count, version int
			require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_objects").Scan(&count))
			require.Zero(t, count)
			require.NoError(t, db.DB.QueryRow("SELECT schema_version FROM storage_meta WHERE id=1").Scan(&version))
			require.Zero(t, version)
			_, err = sqlstorage.New(db.DB, contractDialect(backend), options)
			require.ErrorContains(t, err, "migration")
			// Retry in the parent after rollback / actual process death, using real AES-GCM.
			mappings[0].Transformer = newContractTransformer(t).Transformer
			report, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.NoError(t, err)
			require.Equal(t, 2, report.Checked)
			require.Equal(t, 2, report.Copied)
			require.NotEmpty(t, report.Digest)
			options.Transformer = mappings[0].Transformer
			store, err := sqlstorage.New(db.DB, contractDialect(backend), options)
			require.NoError(t, err)
			list := &example.PodList{}
			require.NoError(t, store.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
			var afterUIDs []string
			var contents []string
			for i := range list.Items {
				afterUIDs = append(afterUIDs, string(list.Items[i].UID))
				contents = append(contents, legacyObjectContent(t, &list.Items[i]))
			}
			sort.Strings(afterUIDs)
			sort.Strings(contents)
			afterContentDigest := legacyStringsDigest(contents)
			require.Equal(t, beforeUIDs, afterUIDs)
			require.Equal(t, beforeContentDigest, afterContentDigest)
			require.NoError(t, store.Close())
			retry, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.NoError(t, err)
			require.Equal(t, report, retry)
			require.Equal(t, sourceBefore, legacySourceDigest(t, db.DB, backend))
			// A full resource mapping with empty backend prefix reads the same keys.
			options.Prefix = ""
			options.ResourcePrefix = "/registry/pods"
			store, err = sqlstorage.New(db.DB, contractDialect(backend), options)
			require.NoError(t, err)
			out := &example.Pod{}
			require.NoError(t, store.Get(context.Background(), "/registry/pods/ns/first", storage.GetOptions{}, out))
			require.Equal(t, "first", string(out.UID))
			require.NoError(t, store.Close())
			// The actual factory must apply /registry only once, including transformer AAD.
			config := factoryConfig(db, backend)
			config.Transformer = mappings[0].Transformer
			factoryStore, destroy, err := createFactory(config)
			require.NoError(t, err)
			require.NoError(t, factoryStore.Get(context.Background(), "/pods/ns/first", storage.GetOptions{}, out))
			destroy()
			var encoded []byte
			require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte("/registry/pods/ns/first")).Scan(&encoded))
			require.NotContains(t, string(encoded), "first")
			_, _, err = mappings[0].Transformer.TransformFromStorage(context.Background(), encoded, value.DefaultContext("/pods/ns/first"))
			require.Error(t, err)
		})
	}
}

type legacyFailureTransformer struct {
	value.Transformer
	calls int
	exit  bool
}

func (tr *legacyFailureTransformer) TransformToStorage(ctx context.Context, data []byte, aad value.Context) ([]byte, error) {
	tr.calls++
	if tr.calls == 2 {
		if tr.exit {
			fmt.Println("migration-copy-barrier")
			os.Exit(23)
		}
		return nil, fmt.Errorf("injected copy error")
	}
	return tr.Transformer.TransformToStorage(ctx, data, aad)
}
func legacyChild(t *testing.T, backend string) {
	driver := "sqlite3"
	table := "keyval"
	if backend == "mysql" {
		driver = "mysql"
		table = "pods"
	}
	db, err := sql.Open(driver, os.Getenv("SQL_LEGACY_DSN"))
	require.NoError(t, err)
	defer db.Close()
	mapping := sqlstorage.LegacyMapping{SourceTable: table, ResourcePrefix: "/registry/pods", NamespaceScoped: true, Codec: contractOptions().Codec, Transformer: &legacyFailureTransformer{Transformer: identity.NewEncryptCheckTransformer(), exit: true}}
	d := contractDialect(backend)
	if os.Getenv("SQL_LEGACY_STAGE") == "ddl-exit" {
		d = legacyExitDialect{d.(sqlstorage.MigrationDialect)}
	}
	_, err = sqlstorage.MigrateLegacy(context.Background(), db, d, []sqlstorage.LegacyMapping{mapping})
	t.Fatalf("child did not reach copy barrier: %T", err)
}
func legacySourceDigest(t *testing.T, db *sql.DB, backend string) string {
	query := "SELECT key,NULL,revision,obj FROM keyval ORDER BY key"
	if backend == "mysql" {
		query = "SELECT name,namespace,revision,obj FROM pods ORDER BY id"
	}
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()
	var entries []string
	for rows.Next() {
		var name, namespace, revision sql.NullString
		var raw []byte
		require.NoError(t, rows.Scan(&name, &namespace, &revision, &raw))
		data, err := json.Marshal([]any{name, namespace, revision, string(raw)})
		require.NoError(t, err)
		entries = append(entries, string(data))
	}
	require.NoError(t, rows.Err())
	return legacyStringsDigest(entries)
}
func legacyStringsDigest(items []string) string {
	raw, _ := json.Marshal(items)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
func legacyObjectContent(t *testing.T, obj runtime.Object) string {
	copy := obj.DeepCopyObject()
	require.NoError(t, (storage.APIObjectVersioner{}).PrepareObjectForStorage(copy))
	raw, err := runtime.Encode(contractOptions().Codec, copy)
	require.NoError(t, err)
	var value any
	require.NoError(t, json.Unmarshal(raw, &value))
	raw, err = json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}
func legacyExpected(t *testing.T, db *sql.DB, backend string) ([]string, string) {
	table := "keyval"
	if backend == "mysql" {
		table = "pods"
	}
	rows, err := db.Query("SELECT obj FROM " + table)
	require.NoError(t, err)
	defer rows.Close()
	var uids, contents []string
	for rows.Next() {
		var raw []byte
		require.NoError(t, rows.Scan(&raw))
		obj := &example.Pod{}
		require.NoError(t, runtime.DecodeInto(contractOptions().Codec, raw, obj))
		uids = append(uids, string(obj.UID))
		contents = append(contents, legacyObjectContent(t, obj))
	}
	require.NoError(t, rows.Err())
	sort.Strings(uids)
	sort.Strings(contents)
	return uids, legacyStringsDigest(contents)
}

// AssertLegacyNamespaceCollision rejects incomplete mappings and ambiguous source identity.
func AssertLegacyNamespaceCollision(t *testing.T, backend string) {
	cases := []string{"missing-mapping", "duplicate-mapping", "missing-transformer", "missing-codec", "corrupt-object", "namespace-empty", "mixed-group", "unknown-field", "existing-live", "nondefault-policy"}
	if backend == "mysql" {
		cases = append(cases, "namespace-null", "null-collision", "unmapped-table")
	} else {
		cases = append(cases, "unmapped-key", "overlap-prefix")
	}
	for _, scenario := range cases {
		t.Run(scenario, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			original := legacySourceDigest(t, db.DB, backend)
			query := "UPDATE keyval SET obj=? WHERE key='/registry/pods/ns/first'"
			if backend == "mysql" {
				query = "UPDATE pods SET obj=? WHERE name='first'"
			}
			switch scenario {
			case "missing-mapping":
				mappings = nil
			case "duplicate-mapping":
				mappings = append(mappings, mappings[0])
			case "missing-transformer":
				mappings[0].Transformer = nil
			case "missing-codec":
				mappings[0].Codec = nil
			case "corrupt-object":
				_, err := db.DB.Exec(query, `{"broken":true}`)
				require.NoError(t, err)
			case "namespace-empty":
				obj := contractPod("first")
				obj.Name = "first"
				obj.Namespace = ""
				raw, err := runtime.Encode(contractOptions().Codec, obj)
				require.NoError(t, err)
				_, err = db.DB.Exec(query, raw)
				require.NoError(t, err)
			case "mixed-group", "unknown-field":
				var raw []byte
				table := "keyval"
				if backend == "mysql" {
					table = "pods"
				}
				require.NoError(t, db.DB.QueryRow("SELECT obj FROM "+table+" LIMIT 1").Scan(&raw))
				obj := map[string]any{}
				require.NoError(t, json.Unmarshal(raw, &obj))
				obj["metadata"].(map[string]any)["name"] = "first"
				if scenario == "mixed-group" {
					obj["apiVersion"] = "other.example/v1"
					mappings[0].Codec = unstructured.UnstructuredJSONScheme
				} else {
					obj["unknownLegacyField"] = "must-not-drop"
				}
				raw, err := json.Marshal(obj)
				require.NoError(t, err)
				_, err = db.DB.Exec(query, raw)
				require.NoError(t, err)
			case "namespace-null":
				_, err := db.DB.Exec("UPDATE pods SET namespace=NULL WHERE name='first'")
				require.NoError(t, err)
			case "null-collision":
				_, err := db.DB.Exec("INSERT INTO pods(name,namespace,revision,obj) SELECT name,NULL,revision,obj FROM pods WHERE name='first'")
				require.NoError(t, err)
			case "unmapped-table":
				_, err := db.DB.Exec("CREATE TABLE other_pods LIKE pods")
				require.NoError(t, err)
			case "unmapped-key":
				_, err := db.DB.Exec("UPDATE keyval SET key='/other/pods/ns/first' WHERE key='/registry/pods/ns/first'")
				require.NoError(t, err)
			case "overlap-prefix":
				other := mappings[0]
				other.ResourcePrefix = "/registry"
				mappings = append(mappings, other)
			case "existing-live":
				// Initialize a target while sources are temporarily moved; never reset a live schema.
				table := "keyval"
				if backend == "mysql" {
					table = "pods"
				}
				_, err := db.DB.Exec("ALTER TABLE " + table + " RENAME TO saved_legacy")
				require.NoError(t, err)
				if backend == "mysql" {
					_, err = db.DB.Exec("ALTER TABLE saved_legacy MODIFY obj LONGTEXT CHARACTER SET utf8mb4 NOT NULL")
					require.NoError(t, err)
				}
				store, err := sqlstorage.New(db.DB, contractDialect(backend), contractOptions())
				require.NoError(t, err)
				createContractPod(t, store, "live", 0)
				require.NoError(t, store.Close())
				if backend == "mysql" {
					_, err = db.DB.Exec("ALTER TABLE saved_legacy MODIFY obj JSON NOT NULL")
					require.NoError(t, err)
				}
				_, err = db.DB.Exec("ALTER TABLE saved_legacy RENAME TO " + table)
				require.NoError(t, err)
			case "nondefault-policy":
				d := contractDialect(backend).(sqlstorage.MigrationDialect)
				require.NoError(t, d.InitializeMigration(context.Background(), db.DB))
				_, err := db.DB.Exec("INSERT INTO storage_policy(id,history_window) VALUES(1,?)", int64(time.Minute))
				require.NoError(t, err)
			}
			before := legacySourceDigest(t, db.DB, backend)
			_, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.Error(t, err)
			require.Equal(t, before, legacySourceDigest(t, db.DB, backend))
			if scenario == "existing-live" {
				var count int
				require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_objects").Scan(&count))
				require.Equal(t, 1, count)
				require.Equal(t, original, before)
			}
			if scenario == "nondefault-policy" {
				var window int64
				require.NoError(t, db.DB.QueryRow("SELECT history_window FROM storage_policy WHERE id=1").Scan(&window))
				require.Equal(t, int64(time.Minute), window)
			}
		})
	}
}

// AssertLegacyWatchBaseline proves relist followed by a real persisted watch update.
func AssertLegacyWatchBaseline(t *testing.T, backend string) {
	db, mappings := legacyFixture(t, backend)
	report, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
	require.NoError(t, err)
	require.Greater(t, report.Baseline, uint64(1))
	o := contractOptions()
	o.Prefix = "/registry"
	store, err := sqlstorage.New(db.DB, contractDialect(backend), o)
	require.NoError(t, err)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old, err := store.Watch(ctx, "/pods", storage.ListOptions{ResourceVersion: "1", Recursive: true, Predicate: storage.Everything})
	require.NoError(t, err)
	defer old.Stop()
	select {
	case event := <-old.ResultChan():
		require.Equal(t, watch.Error, event.Type)
		require.Equal(t, metav1.StatusReasonExpired, event.Object.(*metav1.Status).Reason)
	case <-ctx.Done():
		t.Fatal("old watch did not expire")
	}
	list := &example.PodList{}
	err = store.GetList(ctx, "/pods", storage.ListOptions{Recursive: true, ResourceVersion: "1", ResourceVersionMatch: metav1.ResourceVersionMatchExact, Predicate: storage.Everything}, list)
	require.True(t, apierrors.IsResourceExpired(err))
	require.NoError(t, store.GetList(ctx, "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
	require.Len(t, list.Items, 2)
	require.Equal(t, fmt.Sprint(report.Baseline), list.ResourceVersion)
	next, err := store.Watch(ctx, "/pods", storage.ListOptions{ResourceVersion: list.ResourceVersion, Recursive: true, Predicate: storage.Everything})
	require.NoError(t, err)
	defer next.Stop()
	require.NoError(t, store.GuaranteedUpdate(ctx, "/pods/ns/first", &example.Pod{}, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
		pod := obj.(*example.Pod)
		pod.Labels["updated"] = "yes"
		return pod, nil, nil
	}, nil))
	select {
	case event := <-next.ResultChan():
		require.Equal(t, watch.Modified, event.Type)
		require.Equal(t, "yes", event.Object.(*example.Pod).Labels["updated"])
	case <-ctx.Done():
		t.Fatal("new watch lost post-baseline write")
	}
	require.NoError(t, store.Close())
	_, err = sqlstorage.MigrateLegacy(ctx, db.DB, contractDialect(backend), mappings)
	require.ErrorContains(t, err, "accepted writes")
	o.EventsHistoryWindow = time.Minute
	_, err = sqlstorage.New(db.DB, contractDialect(backend), o)
	require.ErrorContains(t, err, "policy")
	var window int64
	require.NoError(t, db.DB.QueryRow("SELECT history_window FROM storage_policy WHERE id=1").Scan(&window))
	require.Equal(t, int64(storagebackend.DefaultEventsHistoryWindow), window)
}

// AssertLegacyBackupRestore uses a pre-migration backup to restore a fresh database.
func AssertLegacyBackupRestore(t *testing.T, backend string) {
	db, mappings := legacyFixture(t, backend)
	before := legacySourceDigest(t, db.DB, backend)
	directory, err := os.MkdirTemp("/tmp/easehold-upstream-storage", "legacy-backup-")
	require.NoError(t, err)
	defer os.RemoveAll(directory)
	backup := filepath.Join(directory, "backup.db")
	if backend == "sqlite" {
		_, err = db.DB.Exec("VACUUM INTO ?", backup)
		require.NoError(t, err)
	} else {
		// Portable logical SQL backup: fetch source DDL and parameterized rows, then
		// serialize them into a private backup. Restoration executes the saved DDL
		// and bound values against a separate real MySQL database.
		var name, ddl string
		require.NoError(t, db.DB.QueryRow("SHOW CREATE TABLE pods").Scan(&name, &ddl))
		rows, err := db.DB.Query("SELECT id,name,namespace,revision,obj FROM pods ORDER BY id")
		require.NoError(t, err)
		saved := legacyBackup{DDL: ddl}
		for rows.Next() {
			var row legacyBackupRow
			require.NoError(t, rows.Scan(&row.ID, &row.Name, &row.Namespace, &row.Revision, &row.Object))
			saved.Rows = append(saved.Rows, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		raw, err := json.Marshal(saved)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(backup, raw, 0600))
	}
	_, err = sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
	require.NoError(t, err)
	var restored *sql.DB
	if backend == "sqlite" {
		restored, err = sql.Open("sqlite3", backup)
		require.NoError(t, err)
		defer restored.Close()
	} else {
		restore := Open(t, backend)
		restored = restore.DB
		raw, err := os.ReadFile(backup)
		require.NoError(t, err)
		var saved legacyBackup
		require.NoError(t, json.Unmarshal(raw, &saved))
		_, err = restored.Exec(saved.DDL)
		require.NoError(t, err)
		for _, row := range saved.Rows {
			_, err = restored.Exec("INSERT INTO pods(id,name,namespace,revision,obj) VALUES(?,?,?,?,?)", row.ID, row.Name, row.Namespace, row.Revision, row.Object)
			require.NoError(t, err)
		}
	}
	require.Equal(t, before, legacySourceDigest(t, restored, backend))
	_, err = sqlstorage.New(restored, contractDialect(backend), contractOptions())
	require.ErrorContains(t, err, "migration")
	report, err := sqlstorage.MigrateLegacy(context.Background(), restored, contractDialect(backend), mappings)
	require.NoError(t, err)
	require.Equal(t, 2, report.Copied)
}

type legacyBackup struct {
	DDL  string
	Rows []legacyBackupRow
}
type legacyBackupRow struct {
	ID                        int64
	Name, Namespace, Revision sql.NullString
	Object                    []byte
}

type legacyExitDialect struct{ sqlstorage.MigrationDialect }

func (d legacyExitDialect) InitializeMigration(ctx context.Context, db *sql.DB) error {
	if err := d.MigrationDialect.InitializeMigration(ctx, db); err != nil {
		return err
	}
	fmt.Println("migration-copy-barrier")
	os.Exit(23)
	return nil
}

// AssertLegacyCLI exercises the supported shell boundary and private JSON configuration.
func AssertLegacyCLI(t *testing.T, backend string) {
	directory, err := os.MkdirTemp("/tmp/easehold-upstream-storage", "legacy-cli-")
	require.NoError(t, err)
	defer os.RemoveAll(directory)
	_, file, _, ok := goruntime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
	runner := filepath.Join(directory, "runner")
	build := exec.Command("go", "build", "-o", runner, "./cmd/sql-storage-migrate")
	build.Dir = root
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	for _, scenario := range []string{"success", "encrypted", "missing-transformer", "missing-scope", "invalid-config", "public-config", "database-error"} {
		t.Run(scenario, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			transform := map[string]any{"mode": "identity"}
			scope := any(true)
			if scenario == "missing-transformer" {
				transform = map[string]any{}
			}
			if scenario == "missing-scope" {
				scope = nil
			}
			if scenario == "encrypted" {
				keyFile := filepath.Join(directory, "key")
				require.NoError(t, os.WriteFile(keyFile, []byte("BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="), 0600))
				transform = map[string]any{"mode": "aesgcm", "keyFile": keyFile, "keyName": "migration-test"}
			}
			dsn := db.DSN
			if scenario == "database-error" {
				if backend == "mysql" {
					dsn = "invalid@unix(/tmp/easehold-upstream-storage/no-such-mysql.sock)/missing"
				} else {
					dsn = filepath.Join(directory, "missing", "database.db")
				}
			}
			config := map[string]any{"backend": backend, "dsn": dsn, "writersStopped": true, "backupVerified": true, "mappings": []any{map[string]any{"sourceTable": mappings[0].SourceTable, "resourcePrefix": "/registry/pods", "namespaceScoped": scope, "transformation": transform}}}
			raw, err := json.Marshal(config)
			require.NoError(t, err)
			if scenario == "invalid-config" {
				raw = []byte("not JSON")
			}
			configFile := filepath.Join(directory, scenario+".json")
			require.NoError(t, os.WriteFile(configFile, raw, 0600))
			if scenario == "public-config" {
				require.NoError(t, os.Chmod(configFile, 0644))
			}
			command := exec.Command("sh", filepath.Join(root, "cmd/sql-storage-migrate/run.sh"), runner, configFile)
			output, err := command.CombinedOutput()
			if scenario == "success" || scenario == "encrypted" {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), `"ok":true`)
				var count int
				require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM storage_objects").Scan(&count))
				require.Equal(t, 2, count)
				var stored []byte
				require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects LIMIT 1").Scan(&stored))
				if scenario == "encrypted" {
					require.Contains(t, string(stored), "k8s:enc:aesgcm:v1:migration-test:")
					require.NotContains(t, string(stored), "metadata")
				}
			} else {
				require.Error(t, err)
				require.Equal(t, 1, command.ProcessState.ExitCode())
				require.Contains(t, string(output), `"ok":false`)
			}
			require.NotContains(t, string(output), db.DSN)
			require.NotContains(t, string(output), "metadata")
			require.NotContains(t, string(output), "BwcHBwcH")
			// Runner has terminated and released its pool; the source is still readable.
			require.NotEmpty(t, legacySourceDigest(t, db.DB, backend))
		})
	}
}

// AssertLegacyEmptyAndCluster covers empty sources, explicit cluster scope and nonstandard old revisions.
func AssertLegacyEmptyAndCluster(t *testing.T, backend string) {
	cases := []string{"empty", "cluster-empty-namespace", "legacy-revisions"}
	if backend == "mysql" {
		cases = append(cases, "kind-named-keyval")
	}
	for _, scenario := range cases {
		t.Run(scenario, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			table := mappings[0].SourceTable
			if scenario == "empty" {
				_, err := db.DB.Exec("DELETE FROM " + table)
				require.NoError(t, err)
			}
			if scenario == "cluster-empty-namespace" || scenario == "legacy-revisions" {
				for _, name := range []string{"first", "second"} {
					obj := contractPod(name)
					obj.Name = name
					if scenario == "cluster-empty-namespace" {
						obj.Namespace = ""
						mappings[0].NamespaceScoped = false
					} else {
						obj.ResourceVersion = "99"
					}
					raw, err := runtime.Encode(contractOptions().Codec, obj)
					require.NoError(t, err)
					if backend == "mysql" {
						_, err = db.DB.Exec("UPDATE pods SET namespace=?,revision=?,obj=? WHERE name=?", obj.Namespace, 100, raw, name)
					} else {
						key := "/registry/pods/"
						if obj.Namespace != "" {
							key += obj.Namespace + "/"
						}
						_, err = db.DB.Exec("UPDATE keyval SET key=?,revision=?,obj=? WHERE key=?", key+name, 100, raw, "/registry/pods/ns/"+name)
					}
					require.NoError(t, err)
				}
			}
			if scenario == "kind-named-keyval" {
				_, err := db.DB.Exec("ALTER TABLE pods RENAME TO keyval")
				require.NoError(t, err)
				mappings[0].SourceTable = "keyval"
			}
			report, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.NoError(t, err)
			o := contractOptions()
			o.Prefix = "/registry"
			store, err := sqlstorage.New(db.DB, contractDialect(backend), o)
			require.NoError(t, err)
			defer store.Close()
			require.NoError(t, store.ReadinessCheck())
			list := &example.PodList{}
			require.NoError(t, store.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
			if scenario == "empty" {
				require.Empty(t, list.Items)
				require.Zero(t, report.Copied)
				require.Equal(t, uint64(2), report.Baseline)
			} else {
				require.Len(t, list.Items, 2)
			}
			if scenario == "cluster-empty-namespace" {
				out := &example.Pod{}
				require.NoError(t, store.Get(context.Background(), "/pods/first", storage.GetOptions{}, out))
				require.Empty(t, out.Namespace)
				require.Equal(t, "first", string(out.UID))
			}
			if scenario == "legacy-revisions" {
				require.Equal(t, uint64(103), report.Baseline)
				err = store.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, ResourceVersion: "100", ResourceVersionMatch: metav1.ResourceVersionMatchExact, Predicate: storage.Everything}, list)
				require.True(t, apierrors.IsResourceExpired(err))
			}
		})
	}
}

// AssertLegacyReadyIntegrity proves completed retries cannot certify damaged target state.
func AssertLegacyReadyIntegrity(t *testing.T, backend string) {
	cases := map[string]string{
		"compaction-floor":             "UPDATE storage_meta SET compact_revision=0 WHERE id=1",
		"current-expiry":               "UPDATE storage_objects SET expires_at=1",
		"history-expiry":               "UPDATE storage_history SET expires_at=1",
		"history-key-digest":           "UPDATE storage_history SET key_digest=?",
		"history-predecessor-object":   "UPDATE storage_history SET previous_object=?",
		"history-predecessor-expiry":   "UPDATE storage_history SET previous_expires_at=1",
		"history-predecessor-revision": "UPDATE storage_history SET previous_revision=1",
		"history-commit-time":          "UPDATE storage_history SET committed_at=0",
	}
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			_, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.NoError(t, err)
			var args []any
			if name == "history-key-digest" {
				args = []any{make([]byte, 32)}
			}
			if name == "history-predecessor-object" {
				args = []any{[]byte{}}
			}
			_, err = db.DB.Exec(query, args...)
			require.NoError(t, err)
			sourceBefore := legacySourceDigest(t, db.DB, backend)
			targetBefore := legacyTargetDigest(t, db.DB)
			_, err = sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			require.Error(t, err, "damaged completed migration must not be certified")
			require.Equal(t, sourceBefore, legacySourceDigest(t, db.DB, backend))
			require.Equal(t, targetBefore, legacyTargetDigest(t, db.DB))
		})
	}
}

func legacyTargetDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	var contents []string
	for _, table := range []string{"storage_meta", "storage_policy", "storage_migration", "storage_objects", "storage_history"} {
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
			raw, err := json.Marshal(values)
			require.NoError(t, err)
			contents = append(contents, table+":"+string(raw))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return legacyStringsDigest(contents)
}

// AssertLegacyExactNumbers compares raw source numbers independently of target decoding.
func AssertLegacyExactNumbers(t *testing.T, backend string) {
	for _, sample := range []struct {
		name, value   string
		representable bool
	}{
		{"uint64-max", "18446744073709551615", false},
		{"precise-decimal", "0.10000000000000001", false},
		{"exact-integer", "9007199254740993", true},
		{"exact-decimal", "0.125", true},
		{"equivalent-exponent", "1.2500e2", true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			db, mappings := legacyFixture(t, backend)
			mappings[0].Codec = unstructured.UnstructuredJSONScheme
			raw := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"first","namespace":"ns","uid":"first","resourceVersion":"1"},"spec":{"number":` + sample.value + `}}`
			second := strings.ReplaceAll(raw, "first", "second")
			query := "UPDATE keyval SET obj=? WHERE key=?"
			key := "/registry/pods/ns/"
			if backend == "mysql" {
				query = "UPDATE pods SET obj=? WHERE name=?"
				key = ""
			}
			for name, object := range map[string]string{"first": raw, "second": second} {
				_, err := db.DB.Exec(query, object, key+name)
				require.NoError(t, err)
			}
			// MySQL JSON may itself normalize a decimal on ingestion. Restore that exact
			// lexical number with JSON_TYPE DECIMAL storage so the precision-loss probe
			// tests migration rather than the database's earlier binary JSON conversion.
			if backend == "mysql" && sample.name == "precise-decimal" {
				_, err := db.DB.Exec("UPDATE pods SET obj=JSON_SET(obj,'$.spec.number',CAST(? AS DECIMAL(30,17)))", sample.value)
				require.NoError(t, err)
			}
			var storedSource []byte
			require.NoError(t, db.DB.QueryRow("SELECT obj FROM "+mappings[0].SourceTable+" LIMIT 1").Scan(&storedSource))
			if !sample.representable {
				require.Contains(t, string(storedSource), sample.value, "fixture must retain the exact source number")
			}
			require.NoError(t, contractDialect(backend).(sqlstorage.MigrationDialect).InitializeMigration(context.Background(), db.DB))
			sourceBefore := legacySourceDigest(t, db.DB, backend)
			targetBefore := legacyTargetDigest(t, db.DB)
			report, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
			if sample.representable {
				require.NoError(t, err)
				require.Equal(t, 2, report.Copied)
				retry, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
				require.NoError(t, err)
				require.Equal(t, report, retry)
				var targetRaw []byte
				require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte("/registry/pods/ns/first")).Scan(&targetRaw))
				expected := sample.value
				if sample.name == "equivalent-exponent" {
					expected = "125"
				}
				require.Contains(t, string(targetRaw), `"number":`+expected)
				if sample.name == "exact-decimal" {
					corrupted := []byte(strings.ReplaceAll(string(targetRaw), `"number":0.125`, `"number":0.12500000000000001`))
					_, err = db.DB.Exec("UPDATE storage_objects SET object=? WHERE storage_key=?", corrupted, []byte("/registry/pods/ns/first"))
					require.NoError(t, err)
					_, err = db.DB.Exec("UPDATE storage_history SET object=? WHERE storage_key=?", corrupted, []byte("/registry/pods/ns/first"))
					require.NoError(t, err)
					corruptedBefore := legacyTargetDigest(t, db.DB)
					_, err = sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
					require.ErrorContains(t, err, "wire content digest mismatch")
					require.Equal(t, corruptedBefore, legacyTargetDigest(t, db.DB))
				}
			} else {
				require.ErrorContains(t, err, "semantic content")
				require.Equal(t, targetBefore, legacyTargetDigest(t, db.DB))
				o := contractOptions()
				o.Prefix = "/registry"
				_, err = sqlstorage.New(db.DB, contractDialect(backend), o)
				require.ErrorContains(t, err, "migration")
			}
			require.Equal(t, sourceBefore, legacySourceDigest(t, db.DB, backend))
		})
	}
}

// AssertLegacyProtobufCodec retains the real binary codec path alongside typed JSON.
func AssertLegacyProtobufCodec(t *testing.T, backend string) {
	db, mappings := legacyFixture(t, backend)
	scheme := runtime.NewScheme()
	install.Install(scheme)
	codecs := serializer.NewCodecFactory(scheme)
	pb := protobuf.NewSerializer(scheme, scheme)
	codec := codecs.CodecForVersions(pb, codecs.UniversalDeserializer(), schema.GroupVersions{examplev1.SchemeGroupVersion}, schema.GroupVersions{examplev1.SchemeGroupVersion})
	mappings[0].Codec = codec
	if backend == "sqlite" {
		// OLD SQLite can contain an actual non-JSON source as well as a binary target.
		for _, name := range []string{"first", "second"} {
			obj := contractPod(name)
			obj.Name = name
			raw, err := runtime.Encode(codec, obj)
			require.NoError(t, err)
			require.False(t, json.Valid(raw))
			_, err = db.DB.Exec("UPDATE keyval SET obj=? WHERE key=?", raw, "/registry/pods/ns/"+name)
			require.NoError(t, err)
		}
	}
	sourceBefore := legacySourceDigest(t, db.DB, backend)
	report, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
	require.NoError(t, err)
	require.Equal(t, 2, report.Copied)
	var raw []byte
	require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte("/registry/pods/ns/first")).Scan(&raw))
	require.False(t, json.Valid(raw))
	obj, _, err := codec.Decode(raw, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "first", string(obj.(*examplev1.Pod).UID))
	retry, err := sqlstorage.MigrateLegacy(context.Background(), db.DB, contractDialect(backend), mappings)
	require.NoError(t, err)
	require.Equal(t, report, retry)
	require.Equal(t, sourceBefore, legacySourceDigest(t, db.DB, backend))
}
