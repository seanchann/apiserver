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

package sqlstorage

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

func (s *Store) encode(obj runtime.Object) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot store a nil object")
	}
	copy := obj.DeepCopyObject()
	if err := s.versioner.PrepareObjectForStorage(copy); err != nil {
		return nil, err
	}
	return runtime.Encode(s.options.Codec, copy)
}
func (s *Store) decode(data []byte, revision uint64) (runtime.Object, error) {
	obj := s.options.NewFunc()
	if err := runtime.DecodeInto(s.options.Codec, data, obj); err != nil {
		return nil, err
	}
	if err := s.versioner.UpdateObject(obj, revision); err != nil {
		return nil, err
	}
	return obj, nil
}
func (s *Store) validateOutput(out runtime.Object) error {
	if out == nil {
		return fmt.Errorf("output must be a non-nil object pointer")
	}
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Ptr || v.IsNil() || v.Type() != reflect.TypeOf(s.options.NewFunc()) {
		return fmt.Errorf("output must match the configured object type")
	}
	return nil
}
func copyOutput(out, obj runtime.Object) {
	reflect.ValueOf(out).Elem().Set(reflect.ValueOf(obj).Elem())
}

// restore returns nil identity on transformation/decoding failures, matching
// the target unsafe-delete validation contract.
func (s *Store) restore(ctx context.Context, key string, state record) (runtime.Object, []byte, bool, error) {
	plain, stale, err := s.options.Transformer.TransformFromStorage(ctx, state.object, value.DefaultContext(key))
	if err != nil {
		return nil, nil, false, storage.NewInternalError(err)
	}
	obj, err := s.decode(plain, state.revision)
	if err != nil {
		return nil, nil, false, err
	}
	return obj, plain, stale, nil
}

func (s *Store) unsafeDeleteIdentity(ctx context.Context, key string) ([]byte, error) {
	if s.options.ReverseKeyFunc == nil {
		return nil, fmt.Errorf("unsafe delete requires ReverseKeyFunc for history identity")
	}
	name, namespace, err := s.options.ReverseKeyFunc(strings.TrimPrefix(key, s.options.Prefix))
	if err != nil {
		return nil, err
	}
	obj := s.options.NewFunc()
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	// Only key-derived identity is available. In particular, do not invent a UID.
	accessor.SetName(name)
	accessor.SetNamespace(namespace)
	plain, err := s.encode(obj)
	if err != nil {
		return nil, err
	}
	transformed, err := s.options.Transformer.TransformToStorage(ctx, plain, value.DefaultContext(key))
	if err != nil {
		return nil, storage.NewInternalError(err)
	}
	return transformed, nil
}
