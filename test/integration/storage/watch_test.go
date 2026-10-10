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
	"k8s.io/apiserver/test/integration/storage/framework"
	"testing"
)

func TestSQLWatchListBoundary(t *testing.T)  { framework.AssertSQLWatchListBoundary(t, "mysql") }
func TestSQLWatchInitialEvents(t *testing.T) { framework.AssertSQLWatchInitialEvents(t, "mysql") }
func TestSQLWatchPredicateTransitions(t *testing.T) {
	framework.AssertSQLWatchPredicateTransitions(t, "mysql")
}
func TestSQLWatchProgressAndTimestamp(t *testing.T) {
	framework.AssertSQLWatchProgressAndTimestamp(t, "mysql")
}
func TestSQLSlowWatcher(t *testing.T)        { framework.AssertSQLSlowWatcher(t, "mysql") }
func TestSQLWatchStopAndCancel(t *testing.T) { framework.AssertSQLWatchStopAndCancel(t, "mysql") }
func TestSQLWatchReconnectAcrossProcess(t *testing.T) {
	framework.AssertSQLWatchReconnectAcrossProcess(t, "mysql")
}

func TestSQLWatchFutureProgress(t *testing.T) { framework.AssertSQLWatchFutureProgress(t, "mysql") }

func TestSQLWatchSnapshotBoundary(t *testing.T) { framework.AssertSQLWatchSnapshotBoundary(t, "mysql") }

func TestSQLWatchGlobalGaps(t *testing.T) { framework.AssertSQLWatchGlobalGaps(t, "mysql") }

func TestSQLWatchPeriodicProgress(t *testing.T) { framework.AssertSQLWatchPeriodicProgress(t, "mysql") }
