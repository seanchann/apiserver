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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// Create inserts a key without overwriting an existing object.
func (s *Store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, prepareErr := s.prepareKey(key, false)
	if prepareErr != nil {
		return prepareErr
	}
	key = prepared
	if out != nil {
		if err := s.validateOutput(out); err != nil {
			return err
		}
	}
	revision, err := s.versioner.ObjectResourceVersion(obj)
	if err != nil {
		return err
	}
	if revision != 0 {
		return storage.ErrResourceVersionSetOnCreate
	}
	plain, err := s.encode(obj)
	if err != nil {
		return err
	}
	result, err := s.decode(plain, 0)
	if err != nil {
		return err
	}
	transformed, err := s.options.Transformer.TransformToStorage(ctx, plain, value.DefaultContext(key))
	if err != nil {
		return storage.NewInternalError(err)
	}
	expiry, err := s.expiration(ttl)
	if err != nil {
		return err
	}
	revision, err = s.mutate(ctx, key, record{}, transformed, expiry, "ADDED", false)
	if errors.Is(err, errRevisionChanged) {
		return storage.NewKeyExistsError(key, 0)
	}
	if err != nil {
		return err
	}
	if out != nil {
		if err := s.versioner.UpdateObject(result, revision); err != nil {
			return err
		}
		copyOutput(out, result)
	}
	return nil
}

// Get decodes the current object and enforces the requested minimum revision.
func (s *Store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, prepareErr := s.prepareKey(key, false)
	if prepareErr != nil {
		return prepareErr
	}
	key = prepared
	if err := s.validateOutput(out); err != nil {
		return err
	}
	requested, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return err
	}
	state, revision, err := s.read(ctx, key)
	if err != nil {
		return err
	}
	if requested > revision {
		return storage.NewTooLargeResourceVersionError(requested, revision, 0)
	}
	if state.revision == 0 {
		if opts.IgnoreNotFound {
			return runtime.SetZeroValue(out)
		}
		return storage.NewKeyNotFoundError(key, int64(revision))
	}
	obj, _, _, err := s.restore(ctx, key, state)
	if err != nil {
		return err
	}
	copyOutput(out, obj)
	return nil
}

// Delete conditionally removes a key using the target storage API options.
func (s *Store) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, prepareErr := s.prepareKey(key, false)
	if prepareErr != nil {
		return prepareErr
	}
	key = prepared
	if err := s.validateOutput(out); err != nil {
		return err
	}
	unsafe := opts.ExpectTransformOrDecodeError && utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion)
	// Suggested objects are an optimization only; SQL always validates persisted state.
	for {
		state, revision, err := s.read(ctx, key)
		if err != nil {
			return err
		}
		if state.revision == 0 {
			return storage.NewKeyNotFoundError(key, int64(revision))
		}
		obj, _, _, decodeErr := s.restore(ctx, key, state)
		if !unsafe && decodeErr != nil {
			return decodeErr
		}
		if unsafe && decodeErr == nil {
			return storage.NewInvalidObjError(key, "unsafe deletion is not allowed because the object is decodable from storage")
		}
		if err := preconditions.Check(key, obj); err != nil {
			return err
		}
		if err := validateDeletion(ctx, obj); err != nil {
			return err
		}
		if unsafe {
			state.object, err = s.unsafeDeleteIdentity(ctx, key)
			if err != nil {
				return err
			}
		}
		revision, err = s.mutate(ctx, key, state, nil, sql.NullInt64{}, "DELETED", false)
		if errors.Is(err, errRevisionChanged) {
			continue
		}
		if err != nil {
			return err
		}
		if !unsafe {
			if err := s.versioner.UpdateObject(obj, revision); err != nil {
				return err
			}
			copyOutput(out, obj)
		}
		return nil
	}
}

// GuaranteedUpdate runs user callbacks outside transactions and retries revision races.
func (s *Store) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, prepareErr := s.prepareKey(key, false)
	if prepareErr != nil {
		return prepareErr
	}
	key = prepared
	if err := s.validateOutput(destination); err != nil {
		return err
	}
	for {
		state, revision, err := s.read(ctx, key)
		if err != nil {
			return err
		}
		if state.revision == 0 && !ignoreNotFound {
			return storage.NewKeyNotFoundError(key, int64(revision))
		}
		obj := s.options.NewFunc()
		var previousPlain []byte
		var stale bool
		if state.revision != 0 {
			obj, previousPlain, stale, err = s.restore(ctx, key, state)
			if err != nil {
				return err
			}
		}
		if err := preconditions.Check(key, obj); err != nil {
			return err
		}
		response := storage.ResponseMeta{ResourceVersion: state.revision}
		if state.expiresAt.Valid {
			response.TTL = int64(time.Duration(state.expiresAt.Int64-s.options.Clock.Now().UnixNano()) / time.Second)
		}
		updated, ttl, err := tryUpdate(obj, response)
		if err != nil {
			return err
		}
		plain, err := s.encode(updated)
		if err != nil {
			return err
		}
		result, err := s.decode(plain, state.revision)
		if err != nil {
			return err
		}
		expiry := state.expiresAt
		if ttl != nil {
			expiry, err = s.expiration(*ttl)
			if err != nil {
				return err
			}
		}
		noOp := state.revision != 0 && !stale && bytes.Equal(previousPlain, plain) && expiry == state.expiresAt
		var transformed []byte
		if !noOp {
			transformed, err = s.options.Transformer.TransformToStorage(ctx, plain, value.DefaultContext(key))
			if err != nil {
				return storage.NewInternalError(err)
			}
		}
		change := "MODIFIED"
		if state.revision == 0 {
			change = "ADDED"
		}
		revision, err = s.mutate(ctx, key, state, transformed, expiry, change, noOp)
		if errors.Is(err, errRevisionChanged) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.versioner.UpdateObject(result, revision); err != nil {
			return err
		}
		copyOutput(destination, result)
		return nil
	}
}
