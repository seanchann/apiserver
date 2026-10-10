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
	"testing"

	"github.com/stretchr/testify/require"
)

// AssertDatabaseFixtureIsolation verifies separate records and idempotent pool cleanup.
func AssertDatabaseFixtureIsolation(t testing.TB, first, second *Database) {
	t.Helper()
	_, err := first.DB.Exec("CREATE TABLE IF NOT EXISTS fixture_records (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	_, err = second.DB.Exec("CREATE TABLE IF NOT EXISTS fixture_records (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	_, err = first.DB.Exec("INSERT INTO fixture_records (id) VALUES (1)")
	require.NoError(t, err)
	var secondDatabaseCount int
	require.NoError(t, second.DB.QueryRow("SELECT COUNT(*) FROM fixture_records").Scan(&secondDatabaseCount))
	require.Equal(t, 0, secondDatabaseCount)
	require.NoError(t, first.Close())
	pingAfterCloseErr := first.DB.Ping()
	require.Error(t, pingAfterCloseErr)
	require.NoError(t, first.Close())
}
