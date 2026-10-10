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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/test/integration/storage/framework"
)

func TestDatabaseFixtureIsolation(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			first := framework.Open(t, backend)
			second := framework.Open(t, backend)
			var databaseName string
			if backend == "mysql" {
				require.NoError(t, first.DB.QueryRow("SELECT DATABASE()").Scan(&databaseName))
			}
			framework.AssertDatabaseFixtureIsolation(t, first, second)
			if backend == "mysql" {
				var remainingDatabaseCount int
				require.NoError(t, second.DB.QueryRow("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", databaseName).Scan(&remainingDatabaseCount))
				require.Equal(t, 0, remainingDatabaseCount)
			}
		})
	}
}
