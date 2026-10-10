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

package factory

import (
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"strings"
	"time"
)

const sqlDriverTimeout = 10 * time.Second

// mysqlDSN uses the driver's parser so password slashes and query parameters survive.
// Native BEGIN/commit/rollback may outlive request cancellation, so zero deadlines
// are normalized here. Explicit positive deadlines remain the operator's choice.
func mysqlDSN(c storagebackend.MysqlConfig) (string, error) {
	if len(c.ServerList) != 1 || strings.TrimSpace(c.ServerList[0]) == "" {
		return "", fmt.Errorf("MySQL storage requires one server DSN")
	}
	cfg, err := driver.ParseDSN(c.ServerList[0])
	if err != nil {
		return "", fmt.Errorf("invalid MySQL storage DSN; credentials suppressed")
	}
	if cfg.DBName == "" {
		return "", fmt.Errorf("MySQL storage DSN requires a database")
	}
	if cfg.Timeout < 0 || cfg.ReadTimeout < 0 || cfg.WriteTimeout < 0 {
		return "", fmt.Errorf("MySQL storage timeouts must not be negative")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = sqlDriverTimeout
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = sqlDriverTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = sqlDriverTimeout
	}
	return cfg.FormatDSN(), nil
}

func newMysqlStorage(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, reverseKeyFunc storage.ReverseKeyFunc, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	return newSQLStorage(c, newFunc, newListFunc, reverseKeyFunc, resourcePrefix)
}
