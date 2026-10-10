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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

// sqlContinue extends the standard token without hiding fields read by the cacher.
// Raw byte extensions preserve keys that JSON strings cannot represent losslessly.
type sqlContinue struct {
	APIVersion      string `json:"v"`
	ResourceVersion int64  `json:"rv"`
	StartKey        string `json:"start"`
	Prefix          []byte `json:"sqlPrefix,omitempty"`
	Cursor          []byte `json:"sqlCursor,omitempty"`
}

func encodeSQLContinue(cursor, prefix string, revision int64) (string, error) {
	standard, err := storage.EncodeContinue(cursor, prefix, revision)
	if err != nil {
		return "", err
	}
	data, err := base64.RawURLEncoding.DecodeString(standard)
	if err != nil {
		return "", err
	}
	var token sqlContinue
	if err = json.Unmarshal(data, &token); err != nil {
		return "", err
	}
	decodedCursor, _, decodeErr := storage.DecodeContinue(standard, prefix)
	if decodeErr != nil || decodedCursor != cursor {
		// The framework parses start before SQL sees its raw extension. Keep
		// canonical cursors meaningful, but represent noncanonical/raw-byte
		// cursors as one safe component; SQL resumes from Cursor, never start.
		token.StartKey = "~sql-raw~" + base64.RawURLEncoding.EncodeToString([]byte(cursor))
	}
	token.Prefix = []byte(prefix)
	token.Cursor = []byte(cursor)
	data, err = json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(data), err
}
func validateSQLContinue(prefix string, opts storage.ListOptions, versioner storage.Versioner) (uint64, string, error) {
	withRev, cursor, err := storage.ValidateListOptions(prefix, versioner, opts)
	if err != nil {
		return 0, "", err
	}
	if opts.Predicate.Continue != "" {
		if !opts.Recursive {
			return 0, "", apierrors.NewBadRequest("continue requires recursive listing")
		}
		data, err := base64.RawURLEncoding.DecodeString(opts.Predicate.Continue)
		if err != nil {
			return 0, "", apierrors.NewBadRequest("invalid continue encoding")
		}
		var token sqlContinue
		if err = json.Unmarshal(data, &token); err != nil {
			return 0, "", apierrors.NewBadRequest("invalid continue token")
		}
		// Standard tokens from cacher use the upstream relative-cursor semantics.
		if token.Prefix != nil || token.Cursor != nil {
			if string(token.Prefix) != prefix || len(token.Cursor) == 0 || !strings.HasPrefix(string(token.Cursor), prefix) {
				return 0, "", apierrors.NewBadRequest("continue token does not match list prefix")
			}
			cursor = string(token.Cursor)
		}
	}
	if withRev < 0 {
		return 0, "", apierrors.NewBadRequest("resource version exceeds SQL revision range")
	}
	return uint64(withRev), cursor, nil
}

// GetList returns one immutable snapshot; selectors run before the API limit.
// Limit zero is unlimited, irrespective of legacy backend ListDefaultLimit input.
func (s *Store) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix, err := s.prepareKey(key, opts.Recursive)
	if err != nil {
		return err
	}
	if listObj == nil || reflect.TypeOf(listObj) != reflect.TypeOf(s.options.NewListFunc()) || reflect.ValueOf(listObj).IsNil() {
		return fmt.Errorf("list output must match configured list type")
	}
	if opts.Predicate.Limit < 0 {
		return apierrors.NewBadRequest("list limit must not be negative")
	}
	requested, err := s.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(err.Error())
	}
	if requested > math.MaxInt64 {
		return apierrors.NewBadRequest("resource version exceeds SQL revision range")
	}
	revision, cursor, err := validateSQLContinue(prefix, opts, s.versioner)
	if err != nil {
		return err
	}
	records, revision, err := s.readSnapshot(ctx, prefix, opts.Recursive, revision, requested)
	if apierrors.IsResourceExpired(err) && opts.Predicate.Continue != "" {
		expired := apierrors.NewResourceExpired("The provided continue parameter is too old to display a consistent list result. Start a new list without continue, or explicitly use the response token for an inconsistent remainder; intervening creates, updates and deletes may appear.")
		token, tokenErr := encodeSQLContinue(cursor, prefix, -1)
		if tokenErr == nil {
			expired.ErrStatus.ListMeta.Continue = token
		}
		return expired
	}
	if err != nil {
		return err
	}
	items := []runtime.Object{}
	var next string
	var remaining *int64
	for i, item := range records {
		if item.key < cursor {
			continue
		}
		obj, _, _, err := s.restore(ctx, item.key, item.record)
		if err != nil {
			return err
		}
		matches, err := opts.Predicate.Matches(obj)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		items = append(items, obj)
		if opts.Predicate.Limit > 0 && int64(len(items)) == opts.Predicate.Limit && i+1 < len(records) {
			next, err = encodeSQLContinue(item.key+"\x00", prefix, int64(revision))
			if err != nil {
				return err
			}
			if opts.Predicate.Empty() {
				count := int64(len(records) - i - 1)
				remaining = &count
			}
			break
		}
	}
	result := s.options.NewListFunc()
	if err = meta.SetList(result, items); err != nil {
		return err
	}
	if err = s.versioner.UpdateList(result, revision, next, remaining); err != nil {
		return err
	}
	copyOutput(listObj, result)
	return nil
}

// OwnsListPagination keeps SQL snapshot cursors and prefix validation in SQL,
// including the first page which establishes the token's authority.
func (s *Store) OwnsListPagination() bool { return true }
