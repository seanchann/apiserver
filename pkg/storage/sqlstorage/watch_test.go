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
	"k8s.io/apiserver/test/integration/storage/framework"
	"testing"
)

func TestSQLWatchListBoundary(t *testing.T)  { framework.AssertSQLWatchListBoundary(t, "sqlite") }
func TestSQLWatchInitialEvents(t *testing.T) { framework.AssertSQLWatchInitialEvents(t, "sqlite") }
func TestSQLWatchPredicateTransitions(t *testing.T) {
	framework.AssertSQLWatchPredicateTransitions(t, "sqlite")
}
func TestSQLWatchProgressAndTimestamp(t *testing.T) {
	framework.AssertSQLWatchProgressAndTimestamp(t, "sqlite")
}
func TestSQLSlowWatcher(t *testing.T)        { framework.AssertSQLSlowWatcher(t, "sqlite") }
func TestSQLWatchStopAndCancel(t *testing.T) { framework.AssertSQLWatchStopAndCancel(t, "sqlite") }
func TestSQLWatchReconnectAcrossProcess(t *testing.T) {
	framework.AssertSQLWatchReconnectAcrossProcess(t, "sqlite")
}

func TestSQLWatchFutureProgress(t *testing.T) { framework.AssertSQLWatchFutureProgress(t, "sqlite") }

func TestSQLWatchSnapshotBoundary(t *testing.T) {
	framework.AssertSQLWatchSnapshotBoundary(t, "sqlite")
}

func TestSQLWatchGlobalGaps(t *testing.T) { framework.AssertSQLWatchGlobalGaps(t, "sqlite") }

func TestSQLWatchPeriodicProgress(t *testing.T) {
	framework.AssertSQLWatchPeriodicProgress(t, "sqlite")
}
