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

package factory

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"net/url"
	"strconv"
	"strings"
)

const sqliteBusyTimeoutMilliseconds = 5000

func sqliteDSN(c storagebackend.SqliteConfig) (string, error) {
	if strings.TrimSpace(c.DSN) == "" {
		return "", fmt.Errorf("SQLite storage requires a DSN")
	}
	parts := strings.SplitN(c.DSN, "?", 2)
	if strings.TrimSpace(parts[0]) == "" {
		return "", fmt.Errorf("SQLite storage DSN requires a database path")
	}
	values := url.Values{}
	if len(parts) == 2 {
		var err error
		values, err = url.ParseQuery(parts[1])
		if err != nil {
			return "", fmt.Errorf("invalid SQLite storage DSN")
		}
	}
	// Always supply a bounded native busy deadline. The short alias otherwise
	// takes precedence in sqlite3's parser and could defeat this normalization.
	busy := values.Get("_busy_timeout")
	if alias := values.Get("_timeout"); alias != "" {
		busy = alias
	}
	timeout := sqliteBusyTimeoutMilliseconds
	if busy != "" {
		v, err := strconv.Atoi(busy)
		if err != nil || v < 0 {
			return "", fmt.Errorf("invalid SQLite busy timeout")
		}
		if v > 0 {
			timeout = v
		}
	}
	values.Del("_timeout")
	values.Set("_busy_timeout", strconv.Itoa(timeout))
	// Deferred BEGIN lets SQLite wait on its first cancellable write statement.
	if lock := values.Get("_txlock"); lock != "" && lock != "deferred" {
		return "", fmt.Errorf("SQLite storage requires deferred transaction locking")
	}
	values.Set("_txlock", "deferred")
	return parts[0] + "?" + values.Encode(), nil
}

func newSqliteStorage(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, reverseKeyFunc storage.ReverseKeyFunc, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	return newSQLStorage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
}
