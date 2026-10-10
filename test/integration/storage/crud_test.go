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

func TestSQLCreateConflict(t *testing.T) { framework.AssertSQLCreateConflict(t, "mysql") }
func TestSQLConcurrentUpdateWinner(t *testing.T) {
	framework.AssertSQLConcurrentUpdateWinner(t, "mysql")
}
func TestSQLDeleteRecreatePreconditions(t *testing.T) {
	framework.AssertSQLDeleteRecreatePreconditions(t, "mysql")
}
func TestSQLMutationRollback(t *testing.T) { framework.AssertSQLMutationRollback(t, "mysql") }
func TestSQLCodecTransformerRoundTrip(t *testing.T) {
	framework.AssertSQLCodecTransformerRoundTrip(t, "mysql")
}
func TestSQLUnsafeDelete(t *testing.T) { framework.AssertSQLUnsafeDelete(t, "mysql") }
