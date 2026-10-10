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
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

func TestSQLFactoryNativeTimeouts(t *testing.T) {
	for _, query := range []string{"", "?timeout=0s&readTimeout=0s&writeTimeout=0s", "?timeout=3s&readTimeout=4s&writeTimeout=5s"} {
		t.Run(query, func(t *testing.T) {
			cfg := driver.NewConfig()
			cfg.User = "test"
			cfg.Passwd = "password/slash?query"
			cfg.Net = "unix"
			cfg.Addr = "/tmp/easehold-upstream-storage/mysql.sock"
			cfg.DBName = "storage"
			cfg.Params = map[string]string{"charset": "utf8mb4"}
			dsn := cfg.FormatDSN()
			if query != "" {
				dsn += strings.Replace(query, "?", "&", 1)
			}
			normalized, err := mysqlDSN(storagebackend.MysqlConfig{ServerList: []string{dsn}})
			require.NoError(t, err)
			parsed, err := driver.ParseDSN(normalized)
			require.NoError(t, err)
			require.Equal(t, cfg.Passwd, parsed.Passwd)
			require.Equal(t, cfg.Addr, parsed.Addr)
			require.Equal(t, "utf8mb4", parsed.Params["charset"])
			want := []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}
			if strings.Contains(query, "3s") {
				want = []time.Duration{3 * time.Second, 4 * time.Second, 5 * time.Second}
			}
			require.Equal(t, want, []time.Duration{parsed.Timeout, parsed.ReadTimeout, parsed.WriteTimeout})
		})
	}
	for _, query := range []string{"", "?_busy_timeout=0", "?_timeout=0", "?_timeout=23", "?_busy_timeout=19"} {
		normalized, err := sqliteDSN(storagebackend.SqliteConfig{DSN: "/tmp/easehold-upstream-storage/test.db" + query})
		require.NoError(t, err)
		params, err := url.ParseQuery(strings.SplitN(normalized, "?", 2)[1])
		require.NoError(t, err)
		want := "5000"
		if strings.Contains(query, "23") {
			want = "23"
		}
		if strings.Contains(query, "19") {
			want = "19"
		}
		require.Equal(t, want, params.Get("_busy_timeout"))
		require.Empty(t, params.Get("_timeout"))
		require.Equal(t, "deferred", params.Get("_txlock"))
	}
}
func TestSQLFactoryInvalidNativeConfig(t *testing.T) {
	for _, dsns := range [][]string{nil, {""}, {"malformed-secret"}, {"test:secret@unix(/tmp/none)/"}, {"test:secret@tcp(localhost:1)/db?timeout=-1s"}, {"a", "b"}} {
		_, err := mysqlDSN(storagebackend.MysqlConfig{ServerList: dsns})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	for _, dsn := range []string{"", "?_timeout=3", "file:test?bad=%zz", "/tmp/test?_timeout=-1", "/tmp/test?_timeout=bad", "/tmp/test?_txlock=exclusive"} {
		_, err := sqliteDSN(storagebackend.SqliteConfig{DSN: dsn})
		require.Error(t, err)
	}
}
func TestSQLFactoryPoolClose(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp/easehold-upstream-storage", "task6-pool-")
	require.NoError(t, err)
	defer os.RemoveAll(directory)
	c := storagebackend.Config{Type: storagebackend.StorageTypeSqlite, Sqlite: storagebackend.SqliteConfig{DSN: filepath.Join(directory, "db.sqlite")}}
	p, err := newSQLProberMonitor(c)
	require.NoError(t, err)
	require.Positive(t, p.db.Stats().OpenConnections)
	require.NoError(t, p.Close())
	require.NoError(t, p.Close())
	require.Zero(t, p.db.Stats().OpenConnections)
	require.Zero(t, p.db.Stats().InUse)
	require.Error(t, p.db.PingContext(context.Background()))
	_, err = newSQLCheck(c, time.Second, nil)
	require.Error(t, err)
	_, err = newSQLCheck(c, -time.Second, make(chan struct{}))
	require.Error(t, err)
}
