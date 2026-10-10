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

package factory_test

import (
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/test/integration/storage/framework"
	"testing"
)

func TestSQLFactorySelection(t *testing.T)     { framework.AssertSQLFactorySelection(t, "sqlite") }
func TestSQLFactoryInvalidConfig(t *testing.T) { framework.AssertSQLFactoryInvalidConfig(t, "sqlite") }
func TestSQLFactoryHealthLifecycle(t *testing.T) {
	framework.AssertSQLFactoryHealthLifecycle(t, "sqlite")
}
func TestSQLFactoryHistoryPolicy(t *testing.T) { framework.AssertSQLFactoryHistoryPolicy(t, "sqlite") }
func TestSQLFactoryUnknownType(t *testing.T) {
	c := storagebackend.Config{Type: "unknown"}
	stop := make(chan struct{})
	defer close(stop)
	_, _, err := factory.Create(*c.ForResource(schema.GroupResource{Resource: "pods"}), nil, nil, nil, "/pods")
	require.Error(t, err)
	_, err = factory.CreateHealthCheck(c, stop)
	require.Error(t, err)
	_, err = factory.CreateReadyCheck(c, stop)
	require.Error(t, err)
	_, err = factory.CreateProber(c)
	require.Error(t, err)
	_, err = factory.CreateMonitor(c)
	require.Error(t, err)
}

func TestSQLFactoryPrefixContract(t *testing.T) {
	framework.AssertSQLFactoryPrefixContract(t, "sqlite")
}

func TestSQLFactoryDestroyDrain(t *testing.T) { framework.AssertSQLFactoryDestroyDrain(t, "sqlite") }
