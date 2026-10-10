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

	"k8s.io/apiserver/test/integration/storage/framework"
)

func TestSQLTTLUpdateRace(t *testing.T)         { framework.AssertSQLTTLUpdateRace(t, "mysql") }
func TestSQLStatsAndReadiness(t *testing.T)     { framework.AssertSQLStatsAndReadiness(t, "mysql") }
func TestSQLDestroyDuringActivity(t *testing.T) { framework.AssertSQLDestroyDuringActivity(t, "mysql") }

func TestSQLTTLDeterministic(t *testing.T) { framework.AssertSQLTTLDeterministic(t, "mysql") }

func TestSQLHistoryRetention(t *testing.T) { framework.AssertSQLHistoryRetention(t, "mysql") }

func TestSQLDestroyBlockedQuery(t *testing.T) { framework.AssertSQLDestroyBlockedQuery(t, "mysql") }

func TestSQLStatsFailures(t *testing.T) { framework.AssertSQLStatsFailures(t, "mysql") }

func TestSQLUpstreamLifecycle(t *testing.T) { framework.AssertSQLUpstreamLifecycle(t, "mysql") }

func TestSQLTTLProcessRestart(t *testing.T) { framework.AssertSQLTTLProcessRestart(t, "mysql") }

func TestSQLDestroyWorker(t *testing.T) { framework.AssertSQLDestroyWorker(t, "mysql") }

func TestSQLTTLRollback(t *testing.T) { framework.AssertSQLTTLRollback(t, "mysql") }
