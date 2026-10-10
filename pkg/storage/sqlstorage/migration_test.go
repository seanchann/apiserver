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

func TestLegacyMigrationRestartAndRollback(t *testing.T) {
	framework.AssertLegacyMigrationRestartAndRollback(t, "sqlite")
}
func TestLegacyNamespaceCollision(t *testing.T) {
	framework.AssertLegacyNamespaceCollision(t, "sqlite")
}
func TestLegacyWatchBaseline(t *testing.T) { framework.AssertLegacyWatchBaseline(t, "sqlite") }
func TestLegacyBackupRestore(t *testing.T) { framework.AssertLegacyBackupRestore(t, "sqlite") }

func TestLegacyCLI(t *testing.T) { framework.AssertLegacyCLI(t, "sqlite") }

func TestLegacyEmptyAndCluster(t *testing.T) { framework.AssertLegacyEmptyAndCluster(t, "sqlite") }

func TestLegacyReadyIntegrity(t *testing.T) { framework.AssertLegacyReadyIntegrity(t, "sqlite") }

func TestLegacyExactNumbers(t *testing.T) { framework.AssertLegacyExactNumbers(t, "sqlite") }

func TestLegacyProtobufCodec(t *testing.T) { framework.AssertLegacyProtobufCodec(t, "sqlite") }
