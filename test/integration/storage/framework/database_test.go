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

package framework_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/test/integration/storage/framework"
)

func TestDatabaseFixtureIsolation(t *testing.T) {
	first := framework.Open(t, "sqlite")
	second := framework.Open(t, "sqlite")
	framework.AssertDatabaseFixtureIsolation(t, first, second)
}

func TestDatabaseFixtureClose(t *testing.T) {
	database := framework.Open(t, "sqlite")
	require.NoError(t, database.Close())
	require.Error(t, database.DB.Ping())
}

func TestDatabaseFixtureCleanup(t *testing.T) {
	var path string
	t.Run("owned database", func(t *testing.T) {
		database := framework.Open(t, "sqlite")
		path = strings.Split(database.DSN, "?")[0]
		_, err := os.Stat(path)
		require.NoError(t, err)
	})
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDatabaseFixtureRejectsInvalidSelection(t *testing.T) {
	for _, test := range []struct{ name, backend, dsn, message string }{
		{"missing MySQL environment", "mysql", "", "requires SQL_STORAGE_TEST_MYSQL_DSN"},
		{"malformed DSN", "mysql", "sensitive-invalid-credential", "invalid MySQL fixture DSN"},
		{"existing database", "mysql", "fixture:sensitive-invalid-credential@tcp(127.0.0.1:1)/existing", "must not select an existing database"},
		{"unavailable MySQL service", "mysql", "fixture@unix(/tmp/easehold-upstream-storage/nonexistent-test-service.sock)/", "service unavailable or database creation denied"},
		{"unsupported backend", "mongo", "", "unsupported SQL fixture backend"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestDatabaseFixtureFailureHelper$")
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "SQL_STORAGE_TEST_MYSQL_DSN=") && !strings.HasPrefix(value, "SQL_FIXTURE_FAILURE_BACKEND=") {
					command.Env = append(command.Env, value)
				}
			}
			command.Env = append(command.Env, "SQL_FIXTURE_FAILURE_BACKEND="+test.backend, "SQL_STORAGE_TEST_MYSQL_DSN="+test.dsn)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), test.message)
			require.NotContains(t, string(output), "sensitive-invalid-credential")
			require.NotContains(t, string(output), "SKIP")
		})
	}
}

func TestDatabaseFixtureFailureHelper(t *testing.T) {
	if backend := os.Getenv("SQL_FIXTURE_FAILURE_BACKEND"); backend != "" {
		framework.Open(t, backend)
	}
}
