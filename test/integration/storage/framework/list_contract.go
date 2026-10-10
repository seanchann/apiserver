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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
)

type listContract interface {
	GetList(context.Context, string, storage.ListOptions, runtime.Object) error
	GetCurrentResourceVersion(context.Context) (uint64, error)
	CompactRevision() int64
	Compact(context.Context, uint64) (uint64, error)
}

func listAPI(t *testing.T, s *sqlstorage.Store) listContract {
	t.Helper()
	api, ok := any(s).(listContract)
	require.True(t, ok, "SQL Store must implement snapshot List and compaction")
	return api
}
func listOptions() storage.ListOptions {
	return storage.ListOptions{Recursive: true, Predicate: storage.SelectionPredicate{Label: labels.Everything(), Field: fields.Everything()}}
}
func listPods(t *testing.T, s listContract, key string, o storage.ListOptions) *example.PodList {
	t.Helper()
	out := &example.PodList{}
	require.NoError(t, s.GetList(context.Background(), key, o, out))
	return out
}
func createListPod(t *testing.T, s *sqlstorage.Store, key, name string) *example.Pod {
	t.Helper()
	pod := contractPod(name)
	pod.Name = name
	out := &example.Pod{}
	require.NoError(t, s.Create(context.Background(), key, pod, out, 0))
	return out
}

// AssertSQLFilteredPaginationSnapshot catches truncation, filtering-before-limit and moving snapshots.
func AssertSQLFilteredPaginationSnapshot(t *testing.T, backend string) {
	s, _, _ := openContract(t, backend, nil)
	api := listAPI(t, s)
	expected := make([]example.Pod, 0, 1025)
	for i := 0; i < 1025; i++ {
		name := fmt.Sprintf("p%04d", i)
		expected = append(expected, *createListPod(t, s, "/pods/ns/"+name, name))
	}
	o := listOptions()
	full := listPods(t, api, "/pods/ns", o)
	require.Equal(t, expected, full.Items)
	o.Predicate.Limit = 128
	page := listPods(t, api, "/pods/ns", o)
	firstRV := page.ResourceVersion
	require.NotEmpty(t, page.Continue)
	decoded, decodedRV, err := storage.DecodeContinue(page.Continue, "/pods/ns/")
	require.NoError(t, err)
	require.Equal(t, "/pods/ns/p0127\x00", decoded)
	parsedRV, err := strconv.ParseInt(firstRV, 10, 64)
	require.NoError(t, err)
	require.Equal(t, parsedRV, decodedRV)
	compatibility := o
	compatibility.Predicate.Continue = page.Continue
	_, _, err = storage.ValidateListOptions("/pods/ns/", storage.APIObjectVersioner{}, compatibility)
	require.NoError(t, err)
	native, err := storage.EncodeContinue(decoded, "/pods/ns/", decodedRV)
	require.NoError(t, err)
	compatibility.Predicate.Continue = native
	nativePage := listPods(t, api, "/pods/ns", compatibility)
	require.Equal(t, "p0128", nativePage.Items[0].Name)
	require.Equal(t, firstRV, nativePage.ResourceVersion)
	require.EqualValues(t, 897, *page.RemainingItemCount)
	actual := append([]example.Pod{}, page.Items...)
	createListPod(t, s, "/pods/ns/added", "added")
	require.NoError(t, s.Delete(context.Background(), "/pods/ns/p0500", &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}))
	require.NoError(t, s.GuaranteedUpdate(context.Background(), "/pods/ns/p0700", &example.Pod{}, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		obj.(*example.Pod).Labels["changed"] = "yes"
		return obj, nil
	}), nil))
	for page.Continue != "" {
		o.Predicate.Continue = page.Continue
		page = listPods(t, api, "/pods/ns", o)
		require.Equal(t, firstRV, page.ResourceVersion)
		actual = append(actual, page.Items...)
	}
	unique := map[string]bool{}
	for _, p := range actual {
		require.False(t, unique[p.Name])
		unique[p.Name] = true
	}
	require.Len(t, unique, 1025)
	require.Equal(t, expected, actual)
	current := listPods(t, api, "/pods/ns", listOptions())
	require.Len(t, current.Items, 1025)
	currentNames := map[string]example.Pod{}
	for _, item := range current.Items {
		currentNames[item.Name] = item
	}
	require.NotContains(t, currentNames, "p0500")
	require.Contains(t, currentNames, "added")
	require.Equal(t, "yes", currentNames["p0700"].Labels["changed"])
	o = listOptions()
	o.Predicate.Limit = 1
	o.Predicate.Label = labels.SelectorFromSet(labels.Set{"value": "p1024"})
	o.Predicate.GetAttrs = func(obj runtime.Object) (labels.Set, fields.Set, error) {
		return labels.Set(obj.(*example.Pod).Labels), nil, nil
	}
	rare := listPods(t, api, "/pods/ns", o)
	require.Len(t, rare.Items, 1)
	require.Equal(t, "p1024", rare.Items[0].Name)
	require.Nil(t, rare.RemainingItemCount)
	o.Predicate.Label = labels.SelectorFromSet(labels.Set{"value": "absent"})
	require.Empty(t, listPods(t, api, "/pods/ns", o).Items)
}

// AssertSQLBinaryKeyIsolation catches collation folding, truncated keys and prefix leakage.
func AssertSQLBinaryKeyIsolation(t *testing.T, backend string) {
	t.Run("raw-byte-cursors", func(t *testing.T) {
		s, _, _ := openContract(t, backend, nil)
		api := listAPI(t, s)
		for i, key := range []string{"/pods/ns/\xffa", "/pods/ns/\xffb", "/pods/ns/\xffc"} {
			createListPod(t, s, key, strconv.Itoa(i))
		}
		o := listOptions()
		o.Predicate.Limit = 1
		for i := 0; i < 3; i++ {
			page := listPods(t, api, "/pods/ns", o)
			require.Len(t, page.Items, 1)
			require.Equal(t, strconv.Itoa(i), page.Items[0].Name)
			if i < 2 {
				require.NotEmpty(t, page.Continue)
				_, _, err := storage.DecodeContinue(page.Continue, "/pods/ns/")
				require.NoError(t, err)
			} else {
				require.Empty(t, page.Continue)
			}
			o.Predicate.Continue = page.Continue
		}
	})
	s, db, options := openContract(t, backend, nil)
	api := listAPI(t, s)
	keys := []string{"/pods/ns/A", "/pods/ns/a", "/pods/ns/a ", "/pods/ns/a%_", "/pods/ns/" + strings.Repeat("x", 4096) + "a", "/pods/ns/" + strings.Repeat("x", 4096) + "b", "/pods/ns2/a", "/pods/cluster"}
	for i, key := range keys {
		createListPod(t, s, key, strconv.Itoa(i))
	}
	options.ResourcePrefix = "/pods.other"
	other, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer other.Close()
	createListPod(t, other, "/pods.other/ns/a", "other")
	got := listPods(t, api, "/pods/ns", listOptions())
	require.Len(t, got.Items, 6)
	for i, p := range got.Items {
		require.Equal(t, strconv.Itoa(i), p.Name)
	}
	o := listOptions()
	o.Predicate.Limit = 1
	var names []string
	for {
		p := listPods(t, api, "/pods/ns", o)
		for _, item := range p.Items {
			names = append(names, item.Name)
		}
		if p.Continue == "" {
			break
		}
		o.Predicate.Continue = p.Continue
	}
	require.Equal(t, []string{"0", "1", "2", "3", "4", "5"}, names)
	o = listOptions()
	o.Recursive = false
	require.Equal(t, "7", listPods(t, api, "/pods/cluster", o).Items[0].Name)
	require.Empty(t, listPods(t, api, "/pods/missing", o).Items)
	require.Len(t, listPods(t, api, "/pods", listOptions()).Items, 8)
	require.Len(t, listPods(t, listAPI(t, other), "/pods.other", listOptions()).Items, 1)
}

// AssertSQLListRVAndCompaction catches illegal RVs, stale tokens and lost boundary baselines.
func AssertSQLListRVAndCompaction(t *testing.T, backend string) {
	t.Run("decode-transform-and-selector-errors", func(t *testing.T) {
		tr := newContractTransformer(t)
		s, db, _ := openContract(t, backend, func(o *sqlstorage.Options) { o.Transformer = tr })
		api := listAPI(t, s)
		created := createContractPod(t, s, "encrypted", 0)
		require.Equal(t, []example.Pod{*created}, listPods(t, api, "/pods/ns", listOptions()).Items)
		require.False(t, tr.wrongAAD.Load())
		out := &example.PodList{Items: []example.Pod{*contractPod("sentinel")}}
		expected := out.DeepCopy()
		tr.failRead.Store(true)
		require.ErrorIs(t, api.GetList(context.Background(), "/pods/ns", listOptions(), out), transformFailure)
		require.Equal(t, expected, out)
		tr.failRead.Store(false)
		o := listOptions()
		o.Predicate.Label = labels.SelectorFromSet(labels.Set{"value": "encrypted"})
		sentinel := errors.New("selector failed")
		o.Predicate.GetAttrs = func(runtime.Object) (labels.Set, fields.Set, error) { return nil, nil, sentinel }
		require.ErrorIs(t, api.GetList(context.Background(), "/pods/ns", o, out), sentinel)
		require.Equal(t, expected, out)
		_, err := db.DB.Exec("UPDATE storage_history SET object=?", []byte("invalid ciphertext"))
		require.NoError(t, err)
		require.Error(t, api.GetList(context.Background(), "/pods/ns", listOptions(), out))
		require.Equal(t, expected, out)
	})
	t.Run("compaction-rollback-and-tombstones", func(t *testing.T) {
		s, db, _ := openContract(t, backend, nil)
		api := listAPI(t, s)
		createListPod(t, s, "/pods/ns/a", "a")
		require.NoError(t, s.Delete(context.Background(), "/pods/ns/a", &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}))
		latest, err := api.GetCurrentResourceVersion(context.Background())
		require.NoError(t, err)
		before := snapshotContractDB(t, db.DB)
		trigger := "CREATE TRIGGER fail_compact BEFORE DELETE ON storage_history BEGIN SELECT RAISE(ABORT, 'injected compaction failure'); END"
		if backend == "mysql" {
			trigger = "CREATE TRIGGER fail_compact BEFORE DELETE ON storage_history FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected compaction failure'"
		}
		_, err = db.DB.Exec(trigger)
		require.NoError(t, err)
		_, err = api.Compact(context.Background(), latest)
		require.Error(t, err)
		require.Equal(t, before, snapshotContractDB(t, db.DB))
		require.Zero(t, api.CompactRevision())
		_, err = db.DB.Exec("DROP TRIGGER fail_compact")
		require.NoError(t, err)
		actual, err := api.Compact(context.Background(), latest+100)
		require.NoError(t, err)
		require.Equal(t, latest, actual)
		require.Empty(t, listPods(t, api, "/pods/ns", listOptions()).Items)
		actual, err = api.Compact(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, latest, actual)
		createListPod(t, s, "/pods/ns/a", "recreated")
		require.Equal(t, "recreated", listPods(t, api, "/pods/ns", listOptions()).Items[0].Name)
		old := listOptions()
		old.ResourceVersion = strconv.FormatUint(latest, 10)
		old.ResourceVersionMatch = metav1.ResourceVersionMatchExact
		require.Empty(t, listPods(t, api, "/pods/ns", old).Items)
	})

	fake := newMaintenanceClock(time.Unix(1800000000, 0))
	s, db, options := openContract(t, backend, func(o *sqlstorage.Options) { o.Clock = fake })
	fake.wait(t)
	api := listAPI(t, s)
	emptyRV, err := api.GetCurrentResourceVersion(context.Background())
	require.NoError(t, err)
	require.Positive(t, emptyRV)
	createListPod(t, s, "/pods/ns/a", "a")
	createListPod(t, s, "/pods/ns/b", "b")
	o := listOptions()
	o.Predicate.Limit = 1
	first := listPods(t, api, "/pods/ns", o)
	rv, err := strconv.ParseUint(first.ResourceVersion, 10, 64)
	require.NoError(t, err)
	createListPod(t, s, "/pods/ns/c", "c")
	exact := listOptions()
	exact.ResourceVersion = first.ResourceVersion
	exact.ResourceVersionMatch = metav1.ResourceVersionMatchExact
	require.Len(t, listPods(t, api, "/pods/ns", exact).Items, 2)
	exact.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	require.Len(t, listPods(t, api, "/pods/ns", exact).Items, 3)
	for _, bad := range []string{"junk", "-1", "18446744073709551615"} {
		badOpts := listOptions()
		badOpts.ResourceVersion = bad
		require.Error(t, api.GetList(context.Background(), "/pods/ns", badOpts, &example.PodList{}))
	}
	future := listOptions()
	future.ResourceVersion = strconv.FormatUint(rv+100, 10)
	require.True(t, storage.IsTooLargeResourceVersion(api.GetList(context.Background(), "/pods/ns", future, &example.PodList{})))
	o.Predicate.Continue = first.Continue
	require.True(t, apierrors.IsBadRequest(api.GetList(context.Background(), "/pods/ns2", o, &example.PodList{})))
	o.ResourceVersion = first.ResourceVersion
	require.True(t, apierrors.IsBadRequest(api.GetList(context.Background(), "/pods/ns", o, &example.PodList{})))
	o.ResourceVersion = ""
	boundary, err := api.Compact(context.Background(), rv+1)
	require.NoError(t, err)
	require.Equal(t, rv+1, boundary)
	require.EqualValues(t, boundary, api.CompactRevision())
	expired := api.GetList(context.Background(), "/pods/ns", o, &example.PodList{})
	require.True(t, apierrors.IsResourceExpired(expired))
	require.Contains(t, expired.Error(), "The provided continue parameter is too old")
	status := expired.(apierrors.APIStatus).Status()
	require.NotEmpty(t, status.ListMeta.Continue)
	resumed := o
	resumed.Predicate.Continue = status.ListMeta.Continue
	resumedPage := listPods(t, api, "/pods/ns", resumed)
	require.Equal(t, "b", resumedPage.Items[0].Name)
	require.Equal(t, strconv.FormatUint(boundary, 10), resumedPage.ResourceVersion)
	require.NotEmpty(t, resumedPage.Continue)
	exact.ResourceVersionMatch = metav1.ResourceVersionMatchExact
	expiredExact := api.GetList(context.Background(), "/pods/ns", exact, &example.PodList{})
	require.True(t, apierrors.IsResourceExpired(expiredExact))
	require.Contains(t, expiredExact.Error(), "The resourceVersion for the provided list is too old")
	require.Len(t, listPods(t, api, "/pods/ns", listOptions()).Items, 3)
	exact.ResourceVersion = strconv.FormatUint(boundary, 10)
	require.Len(t, listPods(t, api, "/pods/ns", exact).Items, 3)
	reopened, err := sqlstorage.New(db.DB, contractDialect(backend), options)
	require.NoError(t, err)
	defer reopened.Close()
	require.EqualValues(t, boundary, listAPI(t, reopened).CompactRevision())
	require.NoError(t, s.Create(context.Background(), "/pods/ns/ttl", contractPod("ttl"), nil, 1))
	beforeExpiry := listPods(t, api, "/pods/ns", listOptions())
	fake.Step(2 * time.Second)
	fake.cycle(t)
	exact.ResourceVersion = beforeExpiry.ResourceVersion
	require.Equal(t, beforeExpiry.Items, listPods(t, api, "/pods/ns", exact).Items)
	require.NoError(t, s.GuaranteedUpdate(context.Background(), "/pods/ns/c", &example.Pod{}, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		obj.(*example.Pod).Labels["value"] = "updated"
		return obj, nil
	}), nil))
	latest, err := api.GetCurrentResourceVersion(context.Background())
	require.NoError(t, err)
	_, err = api.Compact(context.Background(), latest)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.DB.QueryRow("SELECT count(*) FROM storage_history").Scan(&count))
	require.Equal(t, 4, count)
	require.Len(t, listPods(t, api, "/pods/ns", listOptions()).Items, 3)
}

// AssertSQLListNoncanonicalContinuation verifies accepted raw keys produce consumable tokens.
func AssertSQLListNoncanonicalContinuation(t *testing.T, backend string) {
	for _, firstKey := range []string{"/pods/ns/a//b", "/pods/ns//a", "/pods/ns/a\xff"} {
		t.Run(fmt.Sprintf("key-%x", firstKey), func(t *testing.T) {
			s, _, _ := openContract(t, backend, nil)
			api := listAPI(t, s)
			first := createListPod(t, s, firstKey, "first")
			second := createListPod(t, s, "/pods/ns/z", "second")
			opts := listOptions()
			opts.Predicate.Limit = 1
			page := listPods(t, api, "/pods/ns", opts)
			require.Equal(t, []example.Pod{*first}, page.Items)
			require.NotEmpty(t, page.Continue)
			opts.Predicate.Continue = page.Continue
			withRV, _, err := storage.ValidateListOptions("/pods/ns/", storage.APIObjectVersioner{}, opts)
			require.NoError(t, err)
			require.Equal(t, page.ResourceVersion, strconv.FormatInt(withRV, 10))
			next := listPods(t, api, "/pods/ns", opts)
			require.Equal(t, []example.Pod{*second}, next.Items)
			require.Equal(t, page.ResourceVersion, next.ResourceVersion)
			require.Empty(t, next.Continue)
			require.Equal(t, []example.Pod{*first, *second}, append(page.Items, next.Items...))
			require.True(t, apierrors.IsBadRequest(api.GetList(context.Background(), "/pods/other", opts, &example.PodList{})))

			// Expiration retains the raw cursor in the explicit inconsistent-resume token.
			createListPod(t, s, "/pods/ns/zz", "third")
			latest, err := api.GetCurrentResourceVersion(context.Background())
			require.NoError(t, err)
			_, err = api.Compact(context.Background(), latest)
			require.NoError(t, err)
			expired := api.GetList(context.Background(), "/pods/ns", opts, &example.PodList{})
			require.True(t, apierrors.IsResourceExpired(expired))
			opts.Predicate.Continue = expired.(apierrors.APIStatus).Status().ListMeta.Continue
			require.NotEmpty(t, opts.Predicate.Continue)
			_, _, err = storage.ValidateListOptions("/pods/ns/", storage.APIObjectVersioner{}, opts)
			require.NoError(t, err)
			resumed := listPods(t, api, "/pods/ns", opts)
			require.Equal(t, []example.Pod{*second}, resumed.Items)
			require.Equal(t, strconv.FormatUint(latest, 10), resumed.ResourceVersion)
		})
	}
}
