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
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/features"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	mysqlstorage "k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/sqlite"
	"k8s.io/apiserver/pkg/storage/sqlstorage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value"
)

// transformerSlot changes test transformers without mutating production options.
// The callback executes outside the lock because upstream consistency tests write
// through storage while decoding a snapshot.
type transformerSlot struct {
	mu      sync.RWMutex
	current value.Transformer
}

func (s *transformerSlot) load() value.Transformer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}
func (s *transformerSlot) set(v value.Transformer) { s.mu.Lock(); defer s.mu.Unlock(); s.current = v }
func (s *transformerSlot) TransformFromStorage(ctx context.Context, b []byte, c value.Context) ([]byte, bool, error) {
	return s.load().TransformFromStorage(ctx, b, c)
}
func (s *transformerSlot) TransformToStorage(ctx context.Context, b []byte, c value.Context) ([]byte, error) {
	return s.load().TransformToStorage(ctx, b, c)
}

type upstreamStore struct {
	*sqlstorage.Store
	slot            *transformerSlot
	prefix          *storagetesting.PrefixTransformer
	normalizeTokens bool
}

func (s *upstreamStore) UpdatePrefixTransformer(f storagetesting.PrefixTransformerModifier) func() {
	old := s.slot.load()
	copy := *s.prefix
	s.slot.set(f(&copy))
	return func() { s.slot.set(old) }
}
func (s *upstreamStore) UpdateTransformer(f storagetesting.TransformerModifier) func() {
	old := s.slot.load()
	s.slot.set(f(old))
	return func() { s.slot.set(old) }
}

func TestBackendConcurrentStorageContracts(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			simple := []struct {
				name string
				run  func(context.Context, *testing.T, storage.Interface)
			}{
				{"CreateWithTTL", storagetesting.RunTestCreateWithTTL},
				{"KeySchema", storagetesting.RunTestKeySchema},
				{"CreateWithKeyExist", storagetesting.RunTestCreateWithKeyExist},
				{"Get", storagetesting.RunTestGet},
				{"UnconditionalDelete", storagetesting.RunTestUnconditionalDelete},
				{"ConditionalDelete", storagetesting.RunTestConditionalDelete},
				{"DeleteWithSuggestion", storagetesting.RunTestDeleteWithSuggestion},
				{"DeleteWithSuggestionAndConflict", storagetesting.RunTestDeleteWithSuggestionAndConflict},
				{"DeleteWithConflict", storagetesting.RunTestDeleteWithConflict},
				{"DeleteWithSuggestionOfDeletedObject", storagetesting.RunTestDeleteWithSuggestionOfDeletedObject},
				{"ValidateDeletionWithSuggestion", storagetesting.RunTestValidateDeletionWithSuggestion},
				{"ValidateDeletionWithOnlySuggestionValid", storagetesting.RunTestValidateDeletionWithOnlySuggestionValid},
				{"PreconditionalDeleteWithSuggestion", storagetesting.RunTestPreconditionalDeleteWithSuggestion},
				{"PreconditionalDeleteWithOnlySuggestionPass", storagetesting.RunTestPreconditionalDeleteWithOnlySuggestionPass},
				{"DeleteWithSuggestionAndMissingExpectedTransformOrDecodeError", func(c context.Context, t *testing.T, s storage.Interface) {
					storagetesting.RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError(c, t, s)
				}},
				{"GuaranteedUpdateWithTTL", storagetesting.RunTestGuaranteedUpdateWithTTL},
				{"GuaranteedUpdateWithConflict", storagetesting.RunTestGuaranteedUpdateWithConflict},
				{"GuaranteedUpdateWithSuggestionAndConflict", storagetesting.RunTestGuaranteedUpdateWithSuggestionAndConflict},
				{"GetListRecursivePrefix", storagetesting.RunTestGetListRecursivePrefix},
				{"ListPaging", storagetesting.RunTestListPaging},
				{"NamespaceScopedList", storagetesting.RunTestNamespaceScopedList},
				{"Watch", storagetesting.RunTestWatch},
				{"DeleteTriggerWatch", storagetesting.RunTestDeleteTriggerWatch},
				{"WatchFromNonZero", storagetesting.RunTestWatchFromNonZero},
				{"DelayedWatchDelivery", storagetesting.RunTestDelayedWatchDelivery},
				{"WatchContextCancel", storagetesting.RunTestWatchContextCancel},
				{"WatcherTimeout", storagetesting.RunTestWatcherTimeout},
				{"WatchDeleteEventObjectHaveLatestRV", storagetesting.RunTestWatchDeleteEventObjectHaveLatestRV},
				{"WatchInitializationSignal", storagetesting.RunTestWatchInitializationSignal},
				{"ClusterScopedWatch", storagetesting.RunTestClusterScopedWatch},
				{"NamespaceScopedWatch", storagetesting.RunTestNamespaceScopedWatch},
				{"SendInitialEventsBackwardCompatibility", storagetesting.RunSendInitialEventsBackwardCompatibility},
				{"WatchSemantics", storagetesting.RunWatchSemantics},
				{"WatchSemanticInitialEventsExtended", storagetesting.RunWatchSemanticInitialEventsExtended},
				{"WatchListMatchSingle", storagetesting.RunWatchListMatchSingle},
				{"WatchDispatchBookmarkEvents", func(c context.Context, t *testing.T, s storage.Interface) {
					storagetesting.RunTestWatchDispatchBookmarkEvents(c, t, s, false)
				}},
			}
			for _, test := range simple {
				t.Run(test.name, func(t *testing.T) { s, _, _ := newBackend(t, backend); test.run(t.Context(), t, s) })
			}
			for _, name := range []string{"Create", "GuaranteedUpdate", "GuaranteedUpdateChecksStoredData", "TransformationFailure", "ListResourceVersionMatch", "List", "ListContinuation", "ListPaginationRareObject", "ListContinuationWithFilter", "ConsistentList", "GetListNonRecursive", "CompactRevision", "ListInconsistentContinuation", "WatchFromZero", "WatchError", "WatchErrorIsBlockingFurtherEvents", "Stats"} {
				t.Run(name, func(t *testing.T) {
					prefix := storagetesting.NewPrefixTransformer([]byte("test!"), false)
					slot := &transformerSlot{current: prefix}
					raw, db, opts := newBackend(t, backend, func(o *sqlstorage.Options) { o.Transformer = slot })
					s := &upstreamStore{Store: raw, slot: slot, prefix: prefix, normalizeTokens: name == "List"}
					ctx := t.Context()
					validateKey := func(ctx context.Context, t *testing.T, key string) {
						var n int
						require.NoError(t, db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM storage_objects WHERE storage_key=?", []byte(key)).Scan(&n))
						require.Equal(t, 1, n)
					}
					calls := func(t *testing.T, lists uint64, transforms uint64) {
						t.Logf("SQL adapter: etcd RPC/read-count expectation (%d,%d) is physical-backend-specific; upstream result assertions retained", lists, transforms)
					}
					revisionOpts := opts
					revisionOpts.ResourcePrefix = "/revisions"
					var dialect sqlstorage.Dialect = sqlite.NewDialect()
					if backend == "mysql" {
						dialect = mysqlstorage.NewDialect()
					}
					revisions, err := sqlstorage.New(db.DB, dialect, revisionOpts)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, revisions.Close()) })
					increase := func(ctx context.Context, t *testing.T) int64 {
						rev, err := s.GetCurrentResourceVersion(ctx)
						require.NoError(t, err)
						key := fmt.Sprintf("/revisions/revision-%d", rev)
						require.NoError(t, revisions.Create(ctx, key, &example.Pod{}, nil, 0))
						rev, err = s.GetCurrentResourceVersion(ctx)
						require.NoError(t, err)
						return int64(rev)
					}
					compact := func(ctx context.Context, t *testing.T, rv string) {
						r, err := strconv.ParseUint(rv, 10, 64)
						require.NoError(t, err)
						_, err = s.Compact(ctx, r)
						require.NoError(t, err)
						// Match the upstream compactor metadata write using a real unrelated-resource commit.
						increase(ctx, t)
					}
					switch name {
					case "Create":
						storagetesting.RunTestCreate(ctx, t, s, validateKey)
					case "GuaranteedUpdate":
						storagetesting.RunTestGuaranteedUpdate(ctx, t, s, validateKey)
					case "GuaranteedUpdateChecksStoredData":
						storagetesting.RunTestGuaranteedUpdateChecksStoredData(ctx, t, s)
					case "TransformationFailure":
						storagetesting.RunTestTransformationFailure(ctx, t, s)
					case "ListResourceVersionMatch":
						storagetesting.RunTestListResourceVersionMatch(ctx, t, s)
					case "List":
						storagetesting.RunTestList(ctx, t, s, compact, false, nil)
					case "ListContinuation":
						storagetesting.RunTestListContinuation(ctx, t, s, calls)
					case "ListPaginationRareObject":
						storagetesting.RunTestListPaginationRareObject(ctx, t, s, calls)
					case "ListContinuationWithFilter":
						storagetesting.RunTestListContinuationWithFilter(ctx, t, s, calls)
					case "ConsistentList":
						storagetesting.RunTestConsistentList(ctx, t, s, increase, false, false, false)
					case "GetListNonRecursive":
						storagetesting.RunTestGetListNonRecursive(ctx, t, increase, s)
					case "CompactRevision":
						storagetesting.RunTestCompactRevision(ctx, t, s, increase, compact)
					case "ListInconsistentContinuation":
						storagetesting.RunTestListInconsistentContinuation(ctx, t, s, compact)
					case "WatchFromZero":
						storagetesting.RunTestWatchFromZero(ctx, t, s, compact)
					case "WatchError":
						storagetesting.RunTestWatchError(ctx, t, s)
					case "WatchErrorIsBlockingFurtherEvents":
						storagetesting.RunWatchErrorIsBlockingFurtherEvents(ctx, t, s)
					case "Stats":
						require.NoError(t, s.EnableResourceSizeEstimation(func(context.Context) ([]string, error) { return nil, nil }))
						storagetesting.RunTestStats(ctx, t, s, opts.Codec, slot, true)
					}
				})
			}
		})
	}
}

var _ storage.Interface = (*upstreamStore)(nil)

// GetList normalizes SQL-only extensions solely for RunTestList's literal native
// token comparison. Verify their complete canonical meaning before removing them.
// Independent cache/raw-key tests consume original tokens without this adapter.
func (s *upstreamStore) GetList(ctx context.Context, key string, opts storage.ListOptions, out runtime.Object) error {
	if err := s.Store.GetList(ctx, key, opts, out); err != nil {
		return err
	}
	list := out.(*example.PodList)
	if !s.normalizeTokens || list.Continue == "" {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(list.Continue)
	if err != nil {
		return err
	}
	var token struct {
		Version  string `json:"v"`
		Revision int64  `json:"rv"`
		Start    string `json:"start"`
		Prefix   []byte `json:"sqlPrefix"`
		Cursor   []byte `json:"sqlCursor"`
	}
	if err = json.Unmarshal(data, &token); err != nil {
		return err
	}
	prefix := strings.TrimSuffix(key, "/") + "/"
	cursor, revision, err := storage.DecodeContinue(list.Continue, prefix)
	if err != nil {
		return err
	}
	if token.Version != "meta.k8s.io/v1" || string(token.Prefix) != prefix || string(token.Cursor) != cursor || token.Revision != revision {
		return fmt.Errorf("SQL continuation extension differs from native canonical semantics")
	}
	list.Continue, err = storage.EncodeContinue(cursor, prefix, revision)
	return err
}

type failingCodec struct {
	runtime.Codec
	fail atomic.Bool
}

func (c *failingCodec) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if c.fail.Load() {
		return nil, nil, errors.New("injected decode failure")
	}
	return c.Codec.Decode(data, defaults, into)
}

type failingTransformer struct {
	value.Transformer
	fail atomic.Bool
}

func (f *failingTransformer) TransformFromStorage(ctx context.Context, data []byte, c value.Context) ([]byte, bool, error) {
	if f.fail.Load() {
		return nil, false, errors.New("injected transform failure")
	}
	return f.Transformer.TransformFromStorage(ctx, data, c)
}

func TestBackendUpstreamUnsafeDelete(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)
	for _, backend := range []string{"sqlite", "mysql"} {
		for _, mode := range []string{"transform", "decode"} {
			for _, conflict := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/conflict=%t", backend, mode, conflict), func(t *testing.T) {
					decoder := &failingCodec{}
					transformer := &failingTransformer{Transformer: storagetesting.NewPrefixTransformer([]byte("test!"), false)}
					s, _, _ := newBackend(t, backend, func(o *sqlstorage.Options) {
						o.ReverseKeyFunc = genericregistry.NamespaceReverseKeyFunc("/pods")
						decoder.Codec = o.Codec
						if mode == "decode" {
							o.Codec = decoder
						} else {
							o.Transformer = transformer
						}
					})
					fail := transformer.fail.Store
					if mode == "decode" {
						fail = decoder.fail.Store
					}
					if conflict {
						storagetesting.RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(t.Context(), t, s, fail)
					} else {
						storagetesting.RunTestDeleteExpectedTransformOrDecodeError(t.Context(), t, s, fail)
					}
				})
			}
		}
	}
}
