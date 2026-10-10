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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/utils/clock"
)

// Options configures serialization, resource identity, and history retention.
// The caller owns the connection pool supplied to New.
type Options struct {
	// Prefix namespaces every persisted key and its transformer context.
	Prefix              string
	ResourcePrefix      string
	Codec               runtime.Codec
	Transformer         value.Transformer
	NewFunc             func() runtime.Object
	NewListFunc         func() runtime.Object
	ReverseKeyFunc      storage.ReverseKeyFunc
	EventsHistoryWindow time.Duration
	Clock               clock.WithTicker
}

// DefaultPollInterval is the database history polling cadence.
const DefaultPollInterval = 100 * time.Millisecond
