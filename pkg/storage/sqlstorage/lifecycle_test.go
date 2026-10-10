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

package sqlstorage_test

import (
	"testing"

	"k8s.io/apiserver/test/integration/storage/framework"
)

func TestSQLTTLUpdateRace(t *testing.T)     { framework.AssertSQLTTLUpdateRace(t, "sqlite") }
func TestSQLStatsAndReadiness(t *testing.T) { framework.AssertSQLStatsAndReadiness(t, "sqlite") }
func TestSQLDestroyDuringActivity(t *testing.T) {
	framework.AssertSQLDestroyDuringActivity(t, "sqlite")
}

func TestSQLTTLDeterministic(t *testing.T) { framework.AssertSQLTTLDeterministic(t, "sqlite") }

func TestSQLHistoryRetention(t *testing.T) { framework.AssertSQLHistoryRetention(t, "sqlite") }

func TestSQLDestroyBlockedQuery(t *testing.T) { framework.AssertSQLDestroyBlockedQuery(t, "sqlite") }

func TestSQLStatsFailures(t *testing.T) { framework.AssertSQLStatsFailures(t, "sqlite") }

func TestSQLUpstreamLifecycle(t *testing.T) { framework.AssertSQLUpstreamLifecycle(t, "sqlite") }

func TestSQLTTLProcessRestart(t *testing.T) { framework.AssertSQLTTLProcessRestart(t, "sqlite") }

func TestSQLDestroyWorker(t *testing.T) { framework.AssertSQLDestroyWorker(t, "sqlite") }

func TestSQLTTLRollback(t *testing.T) { framework.AssertSQLTTLRollback(t, "sqlite") }
