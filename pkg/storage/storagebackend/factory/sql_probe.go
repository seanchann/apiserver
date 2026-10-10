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
	"context"
	"database/sql"
	"fmt"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

// sqlProberMonitor owns exactly one pool, independently of resource Stores.
// Its lock prevents pool closure during an admitted check; native I/O is bounded.
type sqlProberMonitor struct {
	mu            sync.RWMutex
	db            *sql.DB
	historyWindow time.Duration
	debug         bool
	closed        bool
}

func newSQLProberMonitor(c storagebackend.Config) (*sqlProberMonitor, error) {
	if c.EventsHistoryWindow < 0 {
		return nil, fmt.Errorf("history window must not be negative")
	}
	db, err := openSQLPool(c)
	if err != nil {
		return nil, err
	}
	window := c.EventsHistoryWindow
	if window == 0 {
		window = storagebackend.DefaultEventsHistoryWindow
	}
	return &sqlProberMonitor{db: db, historyWindow: window, debug: c.Mysql.Debug || c.Sqlite.Debug}, nil
}
func (p *sqlProberMonitor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.db.Close()
}
func (p *sqlProberMonitor) check(ctx context.Context) error {
	var version int
	var revision, compacted, window int64
	var objects, history int
	err := p.db.QueryRowContext(ctx, "SELECT schema_version,revision,compact_revision,(SELECT COUNT(*) FROM storage_objects WHERE 1=0),(SELECT COUNT(*) FROM storage_history WHERE 1=0),(SELECT history_window FROM storage_policy WHERE id=1) FROM storage_meta WHERE id=1").Scan(&version, &revision, &compacted, &objects, &history, &window)
	if err != nil {
		return fmt.Errorf("SQL storage readiness query failed")
	}
	if version != 1 || revision < 1 || compacted < 0 || compacted > revision || window != int64(p.historyWindow) {
		return fmt.Errorf("invalid SQL storage metadata or history window")
	}
	return nil
}
func (p *sqlProberMonitor) Probe(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return fmt.Errorf("SQL storage check is closed")
	}
	started := time.Now()
	err := p.check(ctx)
	if p.debug {
		klog.V(4).InfoS("SQL storage operation", "class", "readiness", "duration", time.Since(started), "count", 1)
	}
	return err
}
func (p *sqlProberMonitor) Monitor(ctx context.Context) (metrics.StorageMetrics, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return metrics.StorageMetrics{}, fmt.Errorf("SQL storage monitor is closed")
	}
	if err := p.check(ctx); err != nil {
		return metrics.StorageMetrics{}, err
	}
	var size int64
	// Length measures persisted bytes for binary blobs on both native drivers.
	if err := p.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(LENGTH(object)),0) FROM storage_objects").Scan(&size); err != nil {
		return metrics.StorageMetrics{}, fmt.Errorf("SQL storage metrics query failed")
	}
	return metrics.StorageMetrics{Size: size}, nil
}
func newSQLCheck(c storagebackend.Config, timeout time.Duration, stopCh <-chan struct{}) (func() error, error) {
	if stopCh == nil {
		return nil, fmt.Errorf("SQL health checks require a shutdown channel")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("SQL health check timeout must be positive")
	}
	p, err := newSQLProberMonitor(c)
	if err != nil {
		return nil, err
	}
	// Both the shutdown waiter and a post-stop invocation join the idempotent
	// Close, making the returned check a deterministic shutdown observation point.
	go func() { <-stopCh; _ = p.Close() }()
	return func() error {
		select {
		case <-stopCh:
			_ = p.Close()
			return fmt.Errorf("SQL storage check is closed")
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return p.Probe(ctx)
	}, nil
}
