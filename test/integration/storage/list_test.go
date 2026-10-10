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

func TestSQLFilteredPaginationSnapshot(t *testing.T) {
	framework.AssertSQLFilteredPaginationSnapshot(t, "mysql")
}
func TestSQLBinaryKeyIsolation(t *testing.T)  { framework.AssertSQLBinaryKeyIsolation(t, "mysql") }
func TestSQLListRVAndCompaction(t *testing.T) { framework.AssertSQLListRVAndCompaction(t, "mysql") }

func TestSQLListNoncanonicalContinuation(t *testing.T) {
	framework.AssertSQLListNoncanonicalContinuation(t, "mysql")
}
