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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/apis/example/install"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher"
	mysqlstorage "k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/test/integration/storage/framework"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

func newBackend(t *testing.T, backend string, configure ...func(*sqlstorage.Options)) (*sqlstorage.Store, *framework.Database, sqlstorage.Options) {
	t.Helper()
	db := framework.Open(t, backend)
	scheme := runtime.NewScheme()
	install.Install(scheme)
	opts := sqlstorage.Options{ResourcePrefix: "/pods", Codec: serializer.NewCodecFactory(scheme).LegacyCodec(examplev1.SchemeGroupVersion), NewFunc: func() runtime.Object { return &example.Pod{} }, NewListFunc: func() runtime.Object { return &example.PodList{} }, EventsHistoryWindow: 2 * time.Minute}
	for _, apply := range configure {
		apply(&opts)
	}
	var dialect sqlstorage.Dialect = sqlite.NewDialect()
	if backend == "mysql" {
		dialect = mysqlstorage.NewDialect()
	}
	s, err := sqlstorage.New(db.DB, dialect, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, db, opts
}

func TestBackendCacheContinuation(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			for _, suffix := range []string{"canonical", "repeated//slash", "raw\xff", strings.Repeat("long", 2048)} {
				t.Run(fmt.Sprintf("cursor-%d", len(suffix)), func(t *testing.T) {
					s, _, opts := newBackend(t, backend)
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					for i, key := range []string{"/pods/ns/" + suffix, "/pods/ns/zzzzzzzz"} {
						pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i), Namespace: "ns", Annotations: map[string]string{"key": base64.RawStdEncoding.EncodeToString([]byte(key))}}}
						require.NoError(t, s.Create(ctx, key, pod, nil, 0))
					}
					attrs := func(obj runtime.Object) (labels.Set, fields.Set, error) {
						p := obj.(*example.Pod)
						return labels.Set(p.Labels), fields.Set{"metadata.name": p.Name, "metadata.namespace": p.Namespace}, nil
					}
					c, err := cacher.NewCacherFromConfig(cacher.Config{Storage: s, Versioner: s.Versioner(), GroupResource: schema.GroupResource{Resource: "pods"}, ResourcePrefix: "/pods", KeyFunc: func(o runtime.Object) (string, error) {
						key, err := base64.RawStdEncoding.DecodeString(o.(*example.Pod).Annotations["key"])
						return string(key), err
					}, GetAttrsFunc: attrs, NewFunc: opts.NewFunc, NewListFunc: opts.NewListFunc, Codec: opts.Codec, EventsHistoryWindow: opts.EventsHistoryWindow})
					require.NoError(t, err)
					defer c.Stop()
					d := cacher.NewCacheDelegator(c, s)
					defer d.Stop()
					require.Eventually(t, c.Ready, 10*time.Second, 10*time.Millisecond)
					t.Run("first-page-owned-by-SQL", func(t *testing.T) {
						cachedOptions := storage.ListOptions{Recursive: true, ResourceVersion: "0", Predicate: storage.Everything}
						cachedOptions.Predicate.Limit = 1
						page := &example.PodList{}
						require.NoError(t, d.GetList(ctx, "/pods/ns", cachedOptions, page))
						require.Len(t, page.Items, 1)
						data, err := base64.RawURLEncoding.DecodeString(page.Continue)
						require.NoError(t, err)
						var token struct {
							Prefix []byte `json:"sqlPrefix"`
							Cursor []byte `json:"sqlCursor"`
						}
						require.NoError(t, json.Unmarshal(data, &token))
						require.Equal(t, "/pods/ns/", string(token.Prefix))
						require.NotEmpty(t, token.Cursor)
					})
					listopts := storage.ListOptions{Recursive: true, Predicate: storage.Everything}
					listopts.Predicate.Limit = 1
					first := &example.PodList{}
					require.NoError(t, d.GetList(ctx, "/pods/ns", listopts, first))
					require.Len(t, first.Items, 1)
					require.NotEmpty(t, first.Continue)
					listopts.Predicate.Continue = first.Continue
					second := &example.PodList{}
					require.NoError(t, d.GetList(ctx, "/pods/ns", listopts, second))
					require.Len(t, second.Items, 1)
					require.NotEqual(t, first.Items[0].Name, second.Items[0].Name)
					require.Equal(t, first.ResourceVersion, second.ResourceVersion)
					err = d.GetList(ctx, "/pods/other", listopts, &example.PodList{})
					require.True(t, apierrors.IsBadRequest(err), "cross-prefix SQL continuation must fail, got %v", err)
					// Also consume a token produced directly by SQL through the actual delegator.
					first = &example.PodList{}
					listopts.Predicate.Continue = ""
					require.NoError(t, s.GetList(ctx, "/pods/ns", listopts, first))
					listopts.Predicate.Continue = first.Continue
					second = &example.PodList{}
					require.NoError(t, d.GetList(ctx, "/pods/ns", listopts, second))
					require.Len(t, second.Items, 1)
					require.NotEqual(t, first.Items[0].Name, second.Items[0].Name)
					require.True(t, apierrors.IsBadRequest(d.GetList(ctx, "/pods/other", listopts, &example.PodList{})))
				})
			}
		})
	}
}

func TestBackendCacheWatchBookmarks(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			s, _, opts := newBackend(t, backend)
			c, err := cacher.NewCacherFromConfig(cacher.Config{Storage: s, Versioner: s.Versioner(), GroupResource: schema.GroupResource{Resource: "pods"}, ResourcePrefix: "/pods", KeyFunc: func(o runtime.Object) (string, error) {
				p := o.(*example.Pod)
				if p.Namespace == "" {
					return "/pods/" + p.Name, nil
				}
				return "/pods/" + p.Namespace + "/" + p.Name, nil
			}, GetAttrsFunc: func(o runtime.Object) (labels.Set, fields.Set, error) {
				p := o.(*example.Pod)
				return labels.Set(p.Labels), fields.Set{"metadata.name": p.Name, "metadata.namespace": p.Namespace}, nil
			}, NewFunc: opts.NewFunc, NewListFunc: opts.NewListFunc, Codec: opts.Codec, EventsHistoryWindow: opts.EventsHistoryWindow})
			require.NoError(t, err)
			defer c.Stop()
			d := cacher.NewCacheDelegator(c, s)
			defer d.Stop()
			require.Eventually(t, c.Ready, 10*time.Second, 10*time.Millisecond)
			storagetesting.RunTestOptionalWatchBookmarksWithCorrectResourceVersion(t.Context(), t, d)
		})
	}
}
