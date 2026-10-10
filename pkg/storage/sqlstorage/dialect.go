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

package sqlstorage

import (
	"context"
	"database/sql"
)

// Dialect isolates the two supported SQL engines' DDL, revision locks and errors.
// LockRevision must acquire a write lock held until the transaction ends.
type Dialect interface {
	Initialize(context.Context, *sql.DB) error
	LockRevision(context.Context, *sql.Tx) (uint64, error)
	IsRetryable(error) bool
	IsDuplicate(error) bool
}
