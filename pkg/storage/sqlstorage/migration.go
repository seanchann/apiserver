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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value"
)

// LegacyMapping explicitly assigns one legacy source to a complete target prefix.
// SQLite accepts multiple disjoint prefixes for keyval. MySQL requires one mapping
// per kind table: mixed API groups cannot be disambiguated by that old schema.
type LegacyMapping struct {
	SourceTable     string
	ResourcePrefix  string
	NamespaceScoped bool
	Codec           runtime.Codec
	Transformer     value.Transformer
}

// MigrationReport exposes integrity evidence, never payloads or connection secrets.
type MigrationReport struct {
	Checked  int
	Copied   int
	Digest   string
	Baseline uint64
}

// MigrationDialect supplies discovery and unpublished DDL for the two SQL engines.
// InitializeMigration must create a staging marker before any target schema DDL.
type MigrationDialect interface {
	Dialect
	LegacyTables(context.Context, *sql.DB) ([]string, error)
	LegacyKeyValue() bool
	InitializeMigration(context.Context, *sql.DB) error
}

// CheckLegacyReady permits retained source tables only after atomic publication.
func CheckLegacyReady(ctx context.Context, db *sql.DB) error {
	var phase string
	var version int
	if err := db.QueryRowContext(ctx, "SELECT m.phase,s.schema_version FROM storage_migration m JOIN storage_meta s ON s.id=m.id WHERE m.id=1").Scan(&phase, &version); err != nil || phase != "ready" || version != 1 {
		return fmt.Errorf("legacy SQL data requires explicit offline migration; target is not ready")
	}
	return nil
}

// InitializeMigrationMarker records intent before MySQL's implicitly committed DDL.
// It never replaces an existing state; all data and readiness publication use DML.
func InitializeMigrationMarker(ctx context.Context, db *sql.DB, mysql bool) error {
	ddl := "CREATE TABLE IF NOT EXISTS storage_migration (id INTEGER PRIMARY KEY, phase VARCHAR(16) NOT NULL, digest VARCHAR(64) NOT NULL, mapping_digest VARCHAR(64) NOT NULL, copied BIGINT NOT NULL, baseline BIGINT NOT NULL)"
	if mysql {
		ddl += " ENGINE=InnoDB"
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	query := "INSERT OR IGNORE INTO storage_migration(id,phase,digest,mapping_digest,copied,baseline) VALUES(1,'staging','','',0,0)"
	if mysql {
		query = "INSERT INTO storage_migration(id,phase,digest,mapping_digest,copied,baseline) VALUES(1,'staging','','',0,0) ON DUPLICATE KEY UPDATE id=id"
	}
	_, err := db.ExecContext(ctx, query)
	return err
}

type legacyRecord struct {
	key      string
	mapping  int
	object   runtime.Object
	semantic []byte
}

// MigrateLegacy requires all writers stopped and an independently verified backup.
// It retains every source table. A failed copy rolls back all target DML; process
// interruption leaves staging metadata that normal startup refuses to serve.
func MigrateLegacy(ctx context.Context, db *sql.DB, dialect Dialect, mappings []LegacyMapping) (MigrationReport, error) {
	var report MigrationReport
	md, ok := dialect.(MigrationDialect)
	if !ok || db == nil {
		return report, fmt.Errorf("migration requires a supported database and dialect")
	}
	tables, err := md.LegacyTables(ctx, db)
	if err != nil {
		return report, err
	}
	fingerprint, err := validateMappings(tables, mappings, md.LegacyKeyValue())
	if err != nil {
		return report, err
	}
	records, maximum, err := readLegacy(ctx, db, tables, mappings, md.LegacyKeyValue())
	if err != nil {
		return report, err
	}
	report.Checked = len(records)
	report.Digest = digestLegacy(records)
	if maximum > math.MaxInt64-uint64(len(records))-2 {
		return report, fmt.Errorf("legacy revision cannot establish a safe baseline")
	}
	report.Baseline = maximum + uint64(len(records)) + 1
	// Existing live schemas (including empty initialized schemas) must not be reset.
	// A ready marker is checked below under the revision lock for an idempotent retry.
	var version int
	if err = db.QueryRowContext(ctx, "SELECT schema_version FROM storage_meta WHERE id=1").Scan(&version); err == nil && version != 0 {
		if err = CheckLegacyReady(ctx, db); err != nil {
			return report, fmt.Errorf("migration refuses an existing live target")
		}
	}
	if err = md.InitializeMigration(ctx, db); err != nil {
		return report, err
	}
	owner := &Store{db: db}
	tx, release, err := owner.beginTransaction(ctx, nil)
	if err != nil {
		return report, err
	}
	defer release()
	revision, err := dialect.LockRevision(ctx, tx)
	if err != nil {
		return report, err
	}
	var phase, oldDigest, oldMapping string
	var copied int
	var baseline uint64
	if err = tx.QueryRowContext(ctx, "SELECT phase,digest,mapping_digest,copied,baseline FROM storage_migration WHERE id=1").Scan(&phase, &oldDigest, &oldMapping, &copied, &baseline); err != nil {
		return report, err
	}
	var compacted uint64
	if err = tx.QueryRowContext(ctx, "SELECT schema_version,compact_revision FROM storage_meta WHERE id=1").Scan(&version, &compacted); err != nil {
		return report, err
	}
	var policy int64
	err = tx.QueryRowContext(ctx, "SELECT history_window FROM storage_policy WHERE id=1").Scan(&policy)
	policyMissing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !policyMissing {
		return report, err
	}
	if err == nil && policy != int64(storagebackend.DefaultEventsHistoryWindow) {
		return report, fmt.Errorf("migration history window differs from persisted database policy")
	}
	if phase == "ready" {
		if version != 1 || revision != baseline || compacted != baseline || oldDigest != report.Digest || oldMapping != fingerprint || copied != len(records) || baseline != report.Baseline || err != nil {
			return report, fmt.Errorf("completed migration differs or target has accepted writes; refusing import")
		}
		if err = verifyMigration(ctx, tx, mappings, records, maximum); err != nil {
			return report, err
		}
		report.Copied = len(records)
		return report, nil
	}
	if phase != "staging" || version != 0 || revision != 1 || compacted != 0 {
		return report, fmt.Errorf("migration target is not an untouched staging schema")
	}
	for _, table := range []string{"storage_objects", "storage_history"} {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return report, err
		}
		if count != 0 {
			return report, fmt.Errorf("migration refuses nonempty staging target")
		}
	}
	for i, item := range records {
		mapping := mappings[item.mapping]
		object := item.object.DeepCopyObject()
		if err = (storage.APIObjectVersioner{}).PrepareObjectForStorage(object); err != nil {
			return report, fmt.Errorf("legacy object cannot be prepared")
		}
		plain, encodeErr := runtime.Encode(mapping.Codec, object)
		if encodeErr != nil {
			return report, fmt.Errorf("legacy target codec encode failed")
		}
		encoded, transformErr := mapping.Transformer.TransformToStorage(ctx, plain, value.DefaultContext(item.key))
		if transformErr != nil {
			return report, fmt.Errorf("legacy target transformation failed")
		}
		objectRevision := maximum + uint64(i) + 2
		if _, err = tx.ExecContext(ctx, "INSERT INTO storage_objects(key_digest,storage_key,resource_prefix,revision,object,expires_at) VALUES(?,?,?,?,?,NULL)", keyDigest(item.key), []byte(item.key), []byte(mapping.ResourcePrefix), objectRevision, encoded); err != nil {
			return report, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO storage_history(revision,key_digest,storage_key,resource_prefix,change_type,previous_object,object,previous_revision,object_revision,previous_expires_at,expires_at,committed_at) VALUES(?,?,?,?,'ADDED',NULL,?,0,?,NULL,NULL,?)", objectRevision, keyDigest(item.key), []byte(item.key), []byte(mapping.ResourcePrefix), encoded, objectRevision, time.Now().UnixNano()); err != nil {
			return report, err
		}
	}
	if err = verifyMigration(ctx, tx, mappings, records, maximum); err != nil {
		return report, err
	}
	// Policy, compaction floor and ready state share the copy transaction.
	if policyMissing {
		if _, err = tx.ExecContext(ctx, "INSERT INTO storage_policy(id,history_window) VALUES(1,?)", int64(storagebackend.DefaultEventsHistoryWindow)); err != nil {
			return report, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET schema_version=1,revision=?,compact_revision=? WHERE id=1", report.Baseline, report.Baseline); err != nil {
		return report, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE storage_migration SET phase='ready',digest=?,mapping_digest=?,copied=?,baseline=? WHERE id=1", report.Digest, fingerprint, len(records), report.Baseline); err != nil {
		return report, err
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Copied = len(records)
	return report, nil
}

func validateMappings(tables []string, mappings []LegacyMapping, keyValue bool) (string, error) {
	if len(tables) == 0 || len(mappings) == 0 {
		return "", fmt.Errorf("legacy migration requires complete explicit mappings")
	}
	available := map[string]bool{}
	for _, table := range tables {
		available[table] = true
	}
	used := map[string]bool{}
	var descriptions []string
	for i, m := range mappings {
		if !available[m.SourceTable] || m.Codec == nil || m.Transformer == nil || m.ResourcePrefix == "/" || !strings.HasPrefix(m.ResourcePrefix, "/") || path.Clean(m.ResourcePrefix) != m.ResourcePrefix || strings.ContainsRune(m.ResourcePrefix, 0) {
			return "", fmt.Errorf("invalid legacy mapping or missing explicit codec/transformer")
		}
		if used[m.SourceTable] && !keyValue {
			return "", fmt.Errorf("duplicate legacy table mapping")
		}
		used[m.SourceTable] = true
		for _, other := range mappings[:i] {
			if m.ResourcePrefix == other.ResourcePrefix || strings.HasPrefix(m.ResourcePrefix, other.ResourcePrefix+"/") || strings.HasPrefix(other.ResourcePrefix, m.ResourcePrefix+"/") {
				return "", fmt.Errorf("duplicate or ambiguous legacy resource prefix")
			}
		}
		descriptions = append(descriptions, fmt.Sprintf("%s:%s:%t", m.SourceTable, m.ResourcePrefix, m.NamespaceScoped))
	}
	if len(used) != len(available) {
		return "", fmt.Errorf("legacy tables have missing mappings")
	}
	sort.Strings(descriptions)
	raw, _ := json.Marshal(descriptions)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func readLegacy(ctx context.Context, db *sql.DB, tables []string, mappings []LegacyMapping, keyValue bool) ([]legacyRecord, uint64, error) {
	var records []legacyRecord
	maximum := uint64(1)
	keys := map[string]bool{}
	groups := map[int]string{}
	for _, table := range tables {
		// Tables come only from catalog discovery; escaping still protects literal names.
		query := "SELECT name,namespace,revision,obj FROM `" + strings.ReplaceAll(table, "`", "``") + "`"
		if keyValue {
			query = "SELECT key,NULL,revision,obj FROM keyval"
		}
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, 0, err
		}
		for rows.Next() {
			var name, namespace, oldRevision sql.NullString
			var raw []byte
			if err = rows.Scan(&name, &namespace, &oldRevision, &raw); err != nil {
				rows.Close()
				return nil, 0, err
			}
			index := -1
			for i, m := range mappings {
				if m.SourceTable == table && (!keyValue || strings.HasPrefix(name.String, m.ResourcePrefix+"/")) {
					if index != -1 {
						rows.Close()
						return nil, 0, fmt.Errorf("ambiguous legacy row mapping")
					}
					index = i
				}
			}
			if index < 0 {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy row has no resource mapping")
			}
			m := mappings[index]
			// Preserve raw JSON numbers before any codec can convert them to float64.
			var sourceSemantic []byte
			if json.Valid(raw) {
				sourceSemantic, err = legacyJSONSemantic(raw)
				if err != nil {
					rows.Close()
					return nil, 0, err
				}
			}
			obj, decodedGVK, decodeErr := m.Codec.Decode(raw, nil, nil)
			if decodeErr != nil {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy object codec decode failed")
			}
			a, accessErr := meta.Accessor(obj)
			if accessErr != nil || a.GetUID() == "" || !safeSegment(a.GetName()) || (m.NamespaceScoped && !safeSegment(a.GetNamespace())) || (!m.NamespaceScoped && a.GetNamespace() != "") {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy object identity or namespace is invalid")
			}
			gvk := decodedGVK
			if gvk == nil {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy object type is missing")
			}
			if gvk.Kind == "" || gvk.Version == "" {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy object type is missing")
			}
			groupKind := gvk.Group + "/" + gvk.Kind
			if previous, ok := groups[index]; ok && previous != groupKind {
				rows.Close()
				return nil, 0, fmt.Errorf("ambiguous legacy API group or kind")
			}
			groups[index] = groupKind
			key := m.ResourcePrefix + "/"
			if m.NamespaceScoped {
				key += a.GetNamespace() + "/"
			}
			key += a.GetName()
			if !name.Valid || (keyValue && name.String != key) || (!keyValue && (name.String != a.GetName() || !namespace.Valid || namespace.String != a.GetNamespace())) {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy key or nullable namespace disagrees with object identity")
			}
			if keys[key] {
				rows.Close()
				return nil, 0, fmt.Errorf("legacy rows collide on target key")
			}
			keys[key] = true
			for _, rv := range []string{oldRevision.String, a.GetResourceVersion()} {
				if n, parseErr := strconv.ParseUint(rv, 10, 64); parseErr == nil && n > maximum {
					maximum = n
				}
			}
			semantic, semanticErr := migrationSemantic(m.Codec, obj)
			if semanticErr != nil {
				rows.Close()
				return nil, 0, semanticErr
			}
			if sourceSemantic != nil && string(sourceSemantic) != string(semantic) {
				rows.Close()
				return nil, 0, fmt.Errorf("target codec changes legacy semantic content")
			}
			records = append(records, legacyRecord{key: key, mapping: index, object: obj, semantic: semantic})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, err
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].key < records[j].key })
	return records, maximum, nil
}
func safeSegment(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\x00")
}
func legacySemantic(obj runtime.Object) ([]byte, error) {
	copy := obj.DeepCopyObject()
	a, err := meta.Accessor(copy)
	if err != nil {
		return nil, fmt.Errorf("legacy object metadata unavailable")
	}
	a.SetResourceVersion("")
	a.SetSelfLink("")
	// Canonicalize through the same JSON representation for typed and unstructured objects.
	raw, err := json.Marshal(copy)
	if err != nil {
		return nil, fmt.Errorf("legacy semantic encoding failed")
	}
	return legacyJSONSemantic(raw)
}

// legacyJSONSemantic canonicalizes the wire bytes independently of runtime codecs.
// Numbers keep their exact decimal value; only RV and selfLink are removed.
func legacyJSONSemantic(raw []byte) ([]byte, error) {
	var canonical any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&canonical); err != nil {
		return nil, fmt.Errorf("legacy semantic JSON failed")
	}
	object, ok := canonical.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("legacy semantic JSON must be an object")
	}
	if metadata, ok := object["metadata"].(map[string]any); ok {
		delete(metadata, "resourceVersion")
		delete(metadata, "selfLink")
	}
	return json.Marshal(normalizeLegacyNumbers(canonical))
}

func normalizeLegacyNumbers(input any) any {
	switch value := input.(type) {
	case json.Number:
		// A decimal coefficient and exponent avoid rounding and avoid expanding large
		// exponents. JSON decoding has already validated the number's grammar.
		text := string(value)
		var exponent big.Int
		if i := strings.IndexAny(text, "eE"); i >= 0 {
			exponent.SetString(text[i+1:], 10)
			text = text[:i]
		}
		sign := ""
		if strings.HasPrefix(text, "-") {
			sign = "-"
			text = text[1:]
		}
		if i := strings.IndexByte(text, '.'); i >= 0 {
			exponent.Sub(&exponent, big.NewInt(int64(len(text)-i-1)))
			text = text[:i] + text[i+1:]
		}
		text = strings.TrimLeft(text, "0")
		if text == "" {
			return json.Number("0")
		}
		coefficient := strings.TrimRight(text, "0")
		exponent.Add(&exponent, big.NewInt(int64(len(text)-len(coefficient))))
		return json.Number(sign + coefficient + "e" + exponent.String())
	case map[string]any:
		for key, item := range value {
			value[key] = normalizeLegacyNumbers(item)
		}
	case []any:
		for i, item := range value {
			value[i] = normalizeLegacyNumbers(item)
		}
	}
	return input
}

func digestLegacy(records []legacyRecord) string {
	h := sha256.New()
	for _, item := range records {
		raw, _ := json.Marshal([]string{item.key, string(item.semantic)})
		h.Write(raw)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func verifyMigration(ctx context.Context, tx *sql.Tx, mappings []LegacyMapping, records []legacyRecord, maximum uint64) error {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM storage_objects").Scan(&count); err != nil {
		return err
	}
	if count != len(records) {
		return fmt.Errorf("migration target count mismatch")
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM storage_history").Scan(&count); err != nil {
		return err
	}
	if count != len(records) {
		return fmt.Errorf("migration history count mismatch")
	}
	for i, item := range records {
		var stored, key, prefix []byte
		var revision uint64
		if err := tx.QueryRowContext(ctx, "SELECT storage_key,resource_prefix,revision,object FROM storage_objects WHERE key_digest=? AND expires_at IS NULL", keyDigest(item.key)).Scan(&key, &prefix, &revision, &stored); err != nil {
			return err
		}
		var historyObject, historyKey, historyPrefix []byte
		var historyRevision uint64
		if err := tx.QueryRowContext(ctx, "SELECT object,storage_key,resource_prefix,object_revision FROM storage_history WHERE revision=? AND key_digest=? AND change_type='ADDED' AND previous_revision=0 AND previous_object IS NULL AND previous_expires_at IS NULL AND expires_at IS NULL AND committed_at>0", revision, keyDigest(item.key)).Scan(&historyObject, &historyKey, &historyPrefix, &historyRevision); err != nil {
			return err
		}
		if string(historyObject) != string(stored) || string(historyKey) != string(key) || string(historyPrefix) != string(prefix) || historyRevision != revision {
			return fmt.Errorf("migration history integrity mismatch")
		}
		m := mappings[item.mapping]
		if string(key) != item.key || string(prefix) != m.ResourcePrefix || revision != maximum+uint64(i)+2 {
			return fmt.Errorf("migration target identity mismatch")
		}
		raw, _, err := m.Transformer.TransformFromStorage(ctx, stored, value.DefaultContext(item.key))
		if err != nil {
			return fmt.Errorf("migration target transform verification failed")
		}
		// Verify JSON ciphertext plaintext before decoding can round a damaged value.
		if json.Valid(raw) {
			semantic, err := legacyJSONSemantic(raw)
			if err != nil || string(semantic) != string(item.semantic) {
				return fmt.Errorf("migration target wire content digest mismatch")
			}
		}
		object, _, err := m.Codec.Decode(raw, nil, nil)
		if err != nil {
			return fmt.Errorf("migration target codec verification failed")
		}
		semantic, err := migrationSemantic(m.Codec, object)
		if err != nil || string(semantic) != string(item.semantic) {
			return fmt.Errorf("migration target content digest mismatch")
		}
	}
	return nil
}

func migrationSemantic(codec runtime.Codec, obj runtime.Object) ([]byte, error) {
	raw, err := runtime.Encode(codec, obj)
	if err != nil {
		return nil, fmt.Errorf("target codec semantic encode failed")
	}
	if !json.Valid(raw) {
		return legacySemantic(obj)
	}
	return legacyJSONSemantic(raw)
}
