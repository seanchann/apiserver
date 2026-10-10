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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
)

func factoryConfig(db *Database, backend string) storagebackend.Config {
	db.DB.SetMaxOpenConns(1)
	db.DB.SetMaxIdleConns(1)
	o := contractOptions()
	return storagebackend.Config{Type: backend, Prefix: "/registry", Codec: o.Codec, Transformer: o.Transformer, Mysql: storagebackend.MysqlConfig{ServerList: []string{db.DSN}, Debug: true, ListDefaultLimit: 1}, Sqlite: storagebackend.SqliteConfig{DSN: db.DSN, Debug: true, ListDefaultLimit: 1}}
}
func createFactory(c storagebackend.Config) (storage.Interface, factory.DestroyFunc, error) {
	o := contractOptions()
	return factory.Create(*c.ForResource(schema.GroupResource{Resource: "pods"}), o.NewFunc, o.NewListFunc, o.ReverseKeyFunc, o.ResourcePrefix)
}

// AssertSQLFactorySelection exercises actual storage, namespace prefixes and unlimited lists.
func AssertSQLFactorySelection(t *testing.T, backend string) {
	db := Open(t, backend)
	c := factoryConfig(db, backend)
	if backend == "mysql" {
		parsed, err := driver.ParseDSN(db.DSN)
		if err != nil {
			t.Fatal("fixture DSN parsing failed")
		}
		parsed.Timeout = 0
		parsed.ReadTimeout = 0
		parsed.WriteTimeout = 0
		native := parsed.FormatDSN()
		separator := "?"
		if strings.Contains(native, "?") {
			separator = "&"
		}
		c.Mysql.ServerList = []string{native + separator + "timeout=0s&readTimeout=0s&writeTimeout=0s"}
	}
	store, destroy, err := createFactory(c)
	require.NoError(t, err)
	defer destroy()
	require.NoError(t, store.ReadinessCheck())
	for _, name := range []string{"first", "second"} {
		obj := contractPod(name)
		obj.Name = name
		require.NoError(t, store.Create(context.Background(), "/pods/ns/"+name, obj, &example.Pod{}, 0))
	}
	list := &example.PodList{}
	require.NoError(t, store.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
	require.Len(t, list.Items, 2)
	var fullKey []byte
	require.NoError(t, db.DB.QueryRow("SELECT storage_key FROM storage_objects WHERE storage_key=?", []byte("/registry/pods/ns/first")).Scan(&fullKey))
	require.Equal(t, "/registry/pods/ns/first", string(fullKey))
	c.Prefix = "/another"
	other, closeOther, err := createFactory(c)
	require.NoError(t, err)
	defer closeOther()
	require.True(t, storage.IsNotFound(other.Get(context.Background(), "/pods/ns/first", storage.GetOptions{}, &example.Pod{})))
	require.NoError(t, other.Create(context.Background(), "/pods/ns/first", &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "ns"}}, nil, 0))
	stats, err := store.Stats(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.ObjectCount)
	destroy()
	destroy()
	require.Error(t, store.ReadinessCheck())
}

// AssertSQLFactoryInvalidConfig proves errors, redaction and failure-pool cleanup.
func AssertSQLFactoryInvalidConfig(t *testing.T, backend string) {
	db := Open(t, backend)
	valid := factoryConfig(db, backend)
	var logs bytes.Buffer
	loggingFlags := flag.NewFlagSet("sql-factory-log-capture", flag.ContinueOnError)
	klog.InitFlags(loggingFlags)
	for _, setting := range []struct{ name, value string }{{"v", "4"}, {"logtostderr", "false"}, {"alsologtostderr", "false"}} {
		name, old := setting.name, loggingFlags.Lookup(setting.name).Value.String()
		require.NoError(t, loggingFlags.Set(name, setting.value))
		defer func() { _ = loggingFlags.Set(name, old) }()
	}
	klog.SetOutput(&logs)
	defer klog.SetOutput(os.Stderr)
	password := "secret/password?query"
	good, closeGood, err := createFactory(valid)
	require.NoError(t, err)
	payload := contractPod(password)
	require.NoError(t, good.Create(context.Background(), contractKey, payload, nil, 0))
	probe, err := factory.CreateProber(valid)
	require.NoError(t, err)
	require.NoError(t, probe.Probe(context.Background()))
	require.NoError(t, probe.Close())
	closeGood()
	invalid := []storagebackend.Config{{Type: backend}}
	bad := valid
	if backend == "mysql" {
		bad.Mysql.ServerList = []string{"user:" + password + "@unix(/tmp/easehold-upstream-storage/absent.sock)/none"}
	} else {
		bad.Sqlite.DSN = "/tmp/easehold-upstream-storage/absent-directory/invalid.db"
	}
	invalid = append(invalid, bad)
	for i, c := range invalid {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var store storage.Interface
			var destroy factory.DestroyFunc
			var err error
			require.NotPanics(t, func() { store, destroy, err = createFactory(c) })
			require.Error(t, err)
			require.Nil(t, store)
			require.Nil(t, destroy)
			require.NotContains(t, err.Error(), password)
			_, err = factory.CreateProber(c)
			require.Error(t, err)
			_, err = factory.CreateMonitor(c)
			require.Error(t, err)
			stop := make(chan struct{})
			defer close(stop)
			_, err = factory.CreateHealthCheck(c, stop)
			require.Error(t, err)
			_, err = factory.CreateReadyCheck(c, stop)
			require.Error(t, err)
		})
	}
	// Constructor failure after opening a real pool must close that pool.
	c := valid
	c.Codec = nil
	baseline := factoryConnectionCount(t, db, backend)
	_, destroy, err := createFactory(c)
	require.Error(t, err)
	require.Nil(t, destroy)
	awaitFactoryConnections(t, db, backend, baseline)
	klog.Flush()
	require.Contains(t, logs.String(), "SQL storage operation")
	require.Contains(t, logs.String(), "class")
	require.NotContains(t, logs.String(), password)
}

// AssertSQLFactoryHealthLifecycle validates live tables and all explicit pool owners.
func AssertSQLFactoryHealthLifecycle(t *testing.T, backend string) {
	db := Open(t, backend)
	c := factoryConfig(db, backend)
	baseline := factoryBaseline(t, db, backend, c)
	store, destroy, err := createFactory(c)
	require.NoError(t, err)
	stop := make(chan struct{})
	health, err := factory.CreateHealthCheck(c, stop)
	require.NoError(t, err)
	ready, err := factory.CreateReadyCheck(c, stop)
	require.NoError(t, err)
	prober, err := factory.CreateProber(c)
	require.NoError(t, err)
	monitor, err := factory.CreateMonitor(c)
	require.NoError(t, err)
	defer destroy()
	defer prober.Close()
	defer monitor.Close()
	require.NoError(t, health())
	require.NoError(t, ready())
	require.NoError(t, prober.Probe(context.Background()))
	m, err := monitor.Monitor(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 0, m.Size)
	require.NoError(t, store.Create(context.Background(), contractKey, contractPod("data"), nil, 0))
	m, err = monitor.Monitor(context.Background())
	require.NoError(t, err)
	require.Positive(t, m.Size)
	// Actual loss of a required database table fails every live readiness path.
	_, err = db.DB.Exec("DROP TABLE storage_history")
	require.NoError(t, err)
	require.Error(t, store.ReadinessCheck())
	require.Error(t, health())
	require.Error(t, ready())
	require.Error(t, prober.Probe(context.Background()))
	_, err = monitor.Monitor(context.Background())
	require.Error(t, err)
	close(stop)
	require.Error(t, health())
	require.Error(t, ready())
	require.NoError(t, prober.Close())
	require.NoError(t, prober.Close())
	require.Error(t, prober.Probe(context.Background()))
	require.NoError(t, monitor.Close())
	require.NoError(t, monitor.Close())
	_, err = monitor.Monitor(context.Background())
	require.Error(t, err)
	var group sync.WaitGroup
	for range 4 {
		group.Go(destroy)
	}
	group.Wait()
	awaitFactoryConnections(t, db, backend, baseline)
}

// AssertSQLFactoryHistoryPolicy checks consistency across independent pools and reopen.
func AssertSQLFactoryHistoryPolicy(t *testing.T, backend string) {
	db := Open(t, backend)
	c := factoryConfig(db, backend)
	baseline := factoryBaseline(t, db, backend, c)
	first, closeFirst, err := createFactory(c)
	require.NoError(t, err)
	defer closeFirst()
	c.EventsHistoryWindow = storagebackend.DefaultEventsHistoryWindow
	second, closeSecond, err := createFactory(c)
	require.NoError(t, err)
	defer closeSecond()
	require.NoError(t, first.ReadinessCheck())
	require.NoError(t, second.ReadinessCheck())
	mismatched := c
	mismatched.EventsHistoryWindow = time.Second
	_, rejectedDestroy, policyErr := createFactory(mismatched)
	require.ErrorContains(t, policyErr, "history window")
	require.Nil(t, rejectedDestroy)
	require.NoError(t, first.ReadinessCheck())
	require.NoError(t, second.ReadinessCheck())
	closeFirst()
	closeSecond()
	c.EventsHistoryWindow = time.Second
	_, destroy, err := createFactory(c)
	require.ErrorContains(t, err, "history window")
	require.Nil(t, destroy)
	c.EventsHistoryWindow = 0
	reopened, closeReopened, err := createFactory(c)
	require.NoError(t, err)
	defer closeReopened()
	require.NoError(t, reopened.ReadinessCheck())
	_, err = db.DB.Exec("DELETE FROM storage_policy")
	require.NoError(t, err)
	require.Error(t, reopened.ReadinessCheck())
	stop := make(chan struct{})
	ready, err := factory.CreateReadyCheck(c, stop)
	require.NoError(t, err)
	require.Error(t, ready())
	close(stop)
	require.Error(t, ready())
	closeReopened()
	awaitFactoryConnections(t, db, backend, baseline)
}
func factoryConnectionCount(t *testing.T, db *Database, backend string) int {
	t.Helper()
	if backend == "mysql" {
		var count int
		require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM information_schema.processlist WHERE DB=DATABASE()").Scan(&count))
		return count
	}
	path := strings.TrimPrefix(strings.Split(db.DSN, "?")[0], "file:")
	files, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	count := 0
	for _, f := range files {
		target, _ := os.Readlink(filepath.Join("/proc/self/fd", f.Name()))
		if target == path {
			count++
		}
	}
	return count
}
func awaitFactoryConnections(t *testing.T, db *Database, backend string, want int) {
	t.Helper()
	// SQLite defers native descriptor closure while any connection holds file locks.
	// Close the observer too before proving no backend descriptor remains.
	if backend == "sqlite" {
		require.NoError(t, db.DB.Close())
		want = 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		actual := factoryConnectionCount(t, db, backend)
		if actual == want {
			return
		}
		if ctx.Err() != nil {
			require.Equal(t, want, actual)
			return
		}
	}
}

func factoryBaseline(t *testing.T, db *Database, backend string, c storagebackend.Config) int {
	t.Helper()
	_, destroy, err := createFactory(c)
	require.NoError(t, err)
	destroy()
	// Let the observer's native connection open WAL sidecars before counting.
	var revision int64
	require.NoError(t, db.DB.QueryRow("SELECT revision FROM storage_meta WHERE id=1").Scan(&revision))
	return factoryConnectionCount(t, db, backend)
}

// AssertSQLFactoryPrefixContract covers all boundaries that consume persisted keys.
func AssertSQLFactoryPrefixContract(t *testing.T, backend string) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)
	db := Open(t, backend)
	c := factoryConfig(db, backend)
	c.Prefix = "registry//"
	transformer := newContractTransformer(t).Transformer
	c.Transformer = transformer
	s, destroy, err := createFactory(c)
	require.NoError(t, err)
	defer destroy()
	for _, name := range []string{"a", "b"} {
		obj := contractPod(name)
		obj.Name = name
		require.NoError(t, s.Create(context.Background(), "/pods/ns/"+name, obj, nil, 0))
	}
	var encrypted []byte
	fullKey := "/registry/pods/ns/a"
	require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte(fullKey)).Scan(&encrypted))
	_, _, err = transformer.TransformFromStorage(context.Background(), encrypted, value.DefaultContext(fullKey))
	require.NoError(t, err)
	_, _, err = transformer.TransformFromStorage(context.Background(), encrypted, value.DefaultContext("/pods/ns/a"))
	require.Error(t, err)
	options := storage.ListOptions{Recursive: true, Predicate: storage.Everything}
	options.Predicate.Limit = 1
	page := &example.PodList{}
	require.NoError(t, s.GetList(context.Background(), "/pods/ns", options, page))
	require.Len(t, page.Items, 1)
	require.NotEmpty(t, page.Continue)
	// Upstream cacher's token parser uses relative resource keys before SQL sees it.
	cursor, _, err := storage.DecodeContinue(page.Continue, "/pods/ns/")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(cursor, "/pods/ns/"))
	options.Predicate.Continue = page.Continue
	next := &example.PodList{}
	require.NoError(t, s.GetList(context.Background(), "/pods/ns", options, next))
	require.Len(t, next.Items, 1)
	require.Equal(t, "b", next.Items[0].Name)
	c.Prefix = "/registry/"
	same, closeSame, err := createFactory(c)
	require.NoError(t, err)
	defer closeSame()
	require.NoError(t, same.Get(context.Background(), "/pods/ns/a", storage.GetOptions{}, &example.Pod{}))
	c.Prefix = "/"
	root, closeRoot, err := createFactory(c)
	require.NoError(t, err)
	defer closeRoot()
	require.Error(t, root.GetList(context.Background(), "/pods/ns", options, &example.PodList{}))
	require.NoError(t, root.Create(context.Background(), "/pods/ns/a", contractPod("root"), nil, 0))
	c.Prefix = ""
	sameRoot, closeSameRoot, err := createFactory(c)
	require.NoError(t, err)
	defer closeSameRoot()
	require.NoError(t, sameRoot.Get(context.Background(), "/pods/ns/a", storage.GetOptions{}, &example.Pod{}))
	list := &example.PodList{}
	require.NoError(t, s.GetList(context.Background(), "/pods", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, list))
	w, err := s.Watch(context.Background(), "/pods", storage.ListOptions{Recursive: true, ResourceVersion: list.ResourceVersion, Predicate: storage.Everything})
	require.NoError(t, err)
	defer w.Stop()
	out := &example.Pod{}
	require.NoError(t, s.GuaranteedUpdate(context.Background(), "/pods/ns/a", out, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		obj.(*example.Pod).Labels["value"] = "updated"
		return obj, nil
	}), nil))
	require.Equal(t, watch.Modified, nextWatch(t, w).Type)
	require.NoError(t, s.Create(context.Background(), "/pods/ns/ttl", contractPod("ttl"), nil, 1))
	require.Equal(t, watch.Added, nextWatch(t, w).Type)
	require.Equal(t, watch.Deleted, nextWatch(t, w).Type)
	require.True(t, storage.IsNotFound(s.Get(context.Background(), "/pods/ns/ttl", storage.GetOptions{}, &example.Pod{})))
	// The expiry candidate is already a full key; it must not be prefixed twice.
	// Unsafe deletion strips only the backend prefix before the registry reverse function.
	_, err = db.DB.Exec("UPDATE storage_objects SET object=? WHERE storage_key=?", []byte("corrupt"), []byte("/registry/pods/ns/b"))
	require.NoError(t, err)
	require.NoError(t, s.Delete(context.Background(), "/pods/ns/b", &example.Pod{}, nil, func(context.Context, runtime.Object) error { return nil }, nil, storage.DeleteOptions{ExpectTransformOrDecodeError: true}))
	deleted := nextWatch(t, w)
	require.Equal(t, watch.Deleted, deleted.Type)
	require.Equal(t, "b", deleted.Object.(*example.Pod).Name)
	require.Equal(t, "ns", deleted.Object.(*example.Pod).Namespace)
	for _, key := range []string{"/pods/ns/raw\x00name", "/pods/ns/" + strings.Repeat("long", 2048), "/pods/ns/non//canonical"} {
		require.NoError(t, s.Create(context.Background(), key, contractPod(key), nil, 0))
		require.NoError(t, s.Get(context.Background(), key, storage.GetOptions{}, &example.Pod{}))
	}
}

// AssertSQLFactoryDestroyDrain proves the factory waits for admitted callbacks before closing its pool.
func AssertSQLFactoryDestroyDrain(t *testing.T, backend string) {
	db := Open(t, backend)
	c := factoryConfig(db, backend)
	baseline := factoryBaseline(t, db, backend, c)
	s, destroy, err := createFactory(c)
	require.NoError(t, err)
	defer destroy()
	require.NoError(t, s.Create(context.Background(), contractKey, contractPod("before"), nil, 0))
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- s.GuaranteedUpdate(context.Background(), contractKey, &example.Pod{}, false, nil, func(obj runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			close(entered)
			<-release
			obj.(*example.Pod).Labels["value"] = "after"
			return obj, nil, nil
		}, nil)
	}()
	<-entered
	destroyed := make(chan struct{})
	go func() { destroy(); close(destroyed) }()
	// Observe sealed admission directly; no elapsed-time assertion or sleep proves drain.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if s.ReadinessCheck() != nil {
			break
		}
		if ctx.Err() != nil {
			close(release)
			t.Fatal("factory destroy did not seal admission")
		}
	}
	select {
	case <-destroyed:
		close(release)
		t.Fatal("destroy returned while callback was still admitted")
	default:
	}
	close(release)
	require.Error(t, <-result)
	<-destroyed
	var stored []byte
	require.NoError(t, db.DB.QueryRow("SELECT object FROM storage_objects WHERE storage_key=?", []byte("/registry"+contractKey)).Scan(&stored))
	require.Contains(t, string(stored), "before")
	require.NotContains(t, string(stored), "after")
	awaitFactoryConnections(t, db, backend, baseline)
}
