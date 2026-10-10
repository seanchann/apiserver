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
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"k8s.io/apiserver/pkg/storage"
)

const transactionRetryDelay = 5 * time.Millisecond

var errRevisionChanged = errors.New("SQL object revision changed")

type record struct {
	revision  uint64
	object    []byte
	expiresAt sql.NullInt64
}

func (s *Store) prepareKey(key string, recursive bool) (string, error) {
	resourcePrefix := s.options.ResourcePrefix
	if !strings.HasSuffix(resourcePrefix, "/") {
		resourcePrefix += "/"
	}
	relative, err := storage.PrepareKey(resourcePrefix, key, recursive)
	if err != nil {
		return "", err
	}
	return s.options.Prefix + relative, nil
}
func (s *Store) storageResourcePrefix() string { return s.options.Prefix + s.options.ResourcePrefix }

func keyDigest(key string) []byte { digest := sha256.Sum256([]byte(key)); return digest[:] }

func (s *Store) expiration(ttl uint64) (sql.NullInt64, error) {
	if ttl == 0 {
		return sql.NullInt64{}, nil
	}
	now := s.options.Clock.Now().UnixNano()
	if ttl > uint64(math.MaxInt64/int64(time.Second)) || now > math.MaxInt64-int64(ttl)*int64(time.Second) {
		return sql.NullInt64{}, fmt.Errorf("TTL overflows SQL expiration timestamp")
	}
	return sql.NullInt64{Int64: now + int64(ttl)*int64(time.Second), Valid: true}, nil
}

func (s *Store) read(ctx context.Context, key string) (record, uint64, error) {
	var state record
	var revision uint64
	var objectRevision sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT m.revision,o.revision,o.object,o.expires_at FROM storage_meta m LEFT JOIN storage_objects o ON o.key_digest=? AND o.storage_key=? WHERE m.id=1", keyDigest(key), []byte(key)).Scan(&revision, &objectRevision, &state.object, &state.expiresAt)
	if err != nil {
		return state, 0, err
	}
	if objectRevision.Valid {
		state.revision = uint64(objectRevision.Int64)
	}
	return state, revision, nil
}

// beginTransaction acquires a cancellable pool lease, then gives this operation
// sole ownership of transaction completion. A cancellable BeginTx context would
// let database/sql roll back asynchronously without a join API, escaping Close's
// drain even when both Tx.Rollback and Conn.Close have returned.
// All statements use ctx. The detached BEGIN handshake relies on bounded driver
// I/O deadlines (and SQLite's busy timeout), configured by the pool owner.
func (s *Store) beginTransaction(ctx context.Context, options *sql.TxOptions) (*sql.Tx, func(), error) {
	connection, err := s.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		connection.Close()
		return nil, nil, err
	}
	tx, err := connection.BeginTx(context.WithoutCancel(ctx), options)
	if err != nil {
		connection.Close()
		return nil, nil, err
	}
	return tx, func() { tx.Rollback(); connection.Close() }, nil
}

// commitTransaction linearizes a completed transaction against closed admission.
// Close waits for an already-started commit; after it seals admission, no new
// commit starts. Callbacks, transformations and SQL statements run outside this lock.
func (s *Store) commitTransaction(ctx context.Context, tx *sql.Tx) error {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	if s.closed {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

// mutate takes only serialized state: callbacks and transformations never run
// while the database-wide revision write lock is held.
func (s *Store) mutate(ctx context.Context, key string, previous record, next []byte, expiry sql.NullInt64, change string, noOp bool) (uint64, error) {
	for {
		revision, err := s.mutateOnce(ctx, key, previous, next, expiry, change, noOp)
		if err == nil || !s.dialect.IsRetryable(err) {
			return revision, err
		}
		timer := time.NewTimer(transactionRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}
func (s *Store) mutateOnce(ctx context.Context, key string, previous record, next []byte, expiry sql.NullInt64, change string, noOp bool) (uint64, error) {
	tx, release, err := s.beginTransaction(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer release()
	revision, err := s.dialect.LockRevision(ctx, tx)
	if err != nil {
		return 0, err
	}
	var current uint64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM storage_objects WHERE key_digest=? AND storage_key=?", keyDigest(key), []byte(key)).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if current != previous.revision {
		return 0, errRevisionChanged
	}
	if noOp {
		return current, s.commitTransaction(ctx, tx)
	}
	if revision >= math.MaxInt64 {
		return 0, fmt.Errorf("SQL storage revision exhausted")
	}
	revision++
	objectRevision := revision
	switch change {
	case "DELETED":
		objectRevision = 0
		_, err = tx.ExecContext(ctx, "DELETE FROM storage_objects WHERE key_digest=? AND storage_key=?", keyDigest(key), []byte(key))
	case "ADDED":
		_, err = tx.ExecContext(ctx, "INSERT INTO storage_objects(key_digest,storage_key,resource_prefix,revision,object,expires_at) VALUES (?,?,?,?,?,?)", keyDigest(key), []byte(key), []byte(s.storageResourcePrefix()), revision, next, expiry)
	default:
		_, err = tx.ExecContext(ctx, "UPDATE storage_objects SET revision=?,object=?,expires_at=? WHERE key_digest=? AND storage_key=?", revision, next, expiry, keyDigest(key), []byte(key))
	}
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET revision=? WHERE id=1", revision); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO storage_history(revision,key_digest,storage_key,resource_prefix,change_type,previous_object,object,previous_revision,object_revision,previous_expires_at,expires_at,committed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", revision, keyDigest(key), []byte(key), []byte(s.storageResourcePrefix()), change, previous.object, next, previous.revision, objectRevision, previous.expiresAt, expiry, s.options.Clock.Now().UnixNano()); err != nil {
		return 0, err
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = s.commitTransaction(ctx, tx); err != nil {
		return 0, err
	}
	return revision, nil
}
