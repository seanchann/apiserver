/*

Copyright 2018 This Project Authors.

Author:  seanchann <seanchann@foxmail.com>

See docs/ for more information about the  project.

*/

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	// apierrors "k8s.io/apimachinery/pkg/api/errors"
	_ "github.com/mattn/go-sqlite3"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog"
	utiltrace "k8s.io/utils/trace"
)

type store struct {
	client           *sql.DB
	codec            runtime.Codec
	versioner        storage.Versioner
	storageVersion   string
	listDefaultLimit int
}

//dataModel
type dataModel struct {
	Key      string
	Revision int64
	Obj      []byte
}

const (
	table = `CREATE TABLE IF NOT EXISTS keyval (
  key TEXT,
  revision interger,
	obj TEXT,
	PRIMARY KEY(key)
 );`

	createSQL   = `INSERT OR REPLACE INTO keyval (key, revision, obj) VALUES (?, ?, ?)`
	updateSQL   = `INSERT OR REPLACE INTO keyval (key, obj) VALUES (?, ?)`
	delSQL      = `DELETE FROM keyval WHERE key=?`
	getSQL      = `SELECT revision,obj FROM keyval WHERE key=?`
	getQuerySQL = `SELECT revision,obj FROM keyval WHERE key=%s`
	getListSQL  = `SELECT revision,obj FROM keyval WHERE key=? LIMIT 1`
	listSQL     = `SELECT revision,obj FROM keyval WHERE key || '/' LIKE ? || '/' || '%' ORDER BY key LIMIT 512`
)

//New create a mysql store
func New(client *sql.DB, codec runtime.Codec, version string, defaultLimit int) storage.Interface {
	return newStore(client, codec, version, defaultLimit)
}

func newStore(client *sql.DB, codec runtime.Codec, version string, defaultLimit int) *store {
	versioner := APIObjectVersioner{}
	if len(version) == 0 {
		klog.Fatalln("need give a storage version for sqlite backend")
	}
	if defaultLimit <= 0 {
		defaultLimit = 1000
	}

	_, err := client.Exec(table)
	if err != nil {
		klog.Fatalln("create sqlite table failure for sqlite backend err %v", err)
	}

	return &store{
		client:           client,
		codec:            codec,
		versioner:        versioner,
		storageVersion:   version,
		listDefaultLimit: defaultLimit,
	}
}

// Versioner implements storage.Interface.Versioner.
func (s *store) Versioner() storage.Versioner {
	return s.versioner
}

func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {

	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion should not be set on objects to be created")
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}

	data := &dataModel{
		Key:      key,
		Revision: 0,
	}

	var err error
	data.Obj, err = runtime.Encode(s.codec, obj)
	if err != nil {
		return storage.NewInternalErrorf("key %v, object encode error %v", key, err.Error())
	}

	stmt, err := s.client.Prepare(createSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}
	defer stmt.Close()

	if _, err = stmt.Exec(data.Key, data.Revision, data.Obj); err != nil {
		return storage.NewInternalErrorf("key %v, transaction exec error %v", key, err.Error())
	}

	return decode(s.codec, s.versioner, data.Obj, out, 0)
}

func (s *store) Delete(
	ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions,
	validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object) error {
	_, err := conversion.EnforcePtr(out)
	if err != nil {
		panic("unable to convert output object to pointer")
	}

	// stmt, err := s.client.Prepare(getSQL)
	// if err != nil {
	// 	return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	// }
	// defer stmt.Close()

	rows, err := s.client.Query(fmt.Sprintf("SELECT revision,obj FROM keyval WHERE key='%s'", key))
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}
	defer rows.Close()

	outData := dataModel{}
	//only fetch one item
	for rows.Next() {
		err := rows.Scan(&outData.Revision, &outData.Obj)
		if err != nil {
			return storage.NewInternalErrorf(key, err.Error())
		}
		break
	}
	if err := decode(s.codec, s.versioner, outData.Obj, out, outData.Revision); err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	if preconditions != nil {
		if err := preconditions.Check(key, out); err != nil {
			return err
		}
	}

	if err := validateDeletion(ctx, out); err != nil {
		return err
	}

	stmt, err := s.client.Prepare(delSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(key)
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	return nil
}

func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, storage.NewInternalError(fmt.Sprintf("the backend of mysql not support watch"))
}

func (s *store) WatchList(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, storage.NewInternalError(fmt.Sprintf("the backend of mysql not support watch"))
}

func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions,
	out runtime.Object) error {

	stmt, err := s.client.Prepare(getSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}
	defer rows.Close()

	outData := dataModel{}
	//only fetch one item
	for rows.Next() {
		err := rows.Scan(&outData.Revision, &outData.Obj)
		if err != nil {
			return storage.NewInternalErrorf(key, err.Error())
		}
		break
	}

	return decode(s.codec, s.versioner, outData.Obj, out, outData.Revision)
}

func (s *store) GetToList(ctx context.Context, key string,
	opts storage.ListOptions, listObj runtime.Object) error {

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		panic("need ptr to slice")
	}

	stmt, err := s.client.Prepare(getListSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		item := dataModel{}
		err := rows.Scan(&item.Revision, &item.Obj)
		if err != nil {
			return storage.NewInternalErrorf(key, err.Error())
		}
		if err := appendListItem(v, item.Obj, uint64(0), opts.Predicate, s.codec, s.versioner); err != nil {
			return err
		}

		break
	}

	// update version with cluster level revision
	return s.versioner.UpdateList(listObj, uint64(0), "", nil)
}

func (s *store) List(ctx context.Context, key string,
	opts storage.ListOptions, listObj runtime.Object) error {

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		panic("need ptr to slice")
	}

	stmt, err := s.client.Prepare(listSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		item := dataModel{}
		err := rows.Scan(&item.Revision, &item.Obj)
		if err != nil {
			return storage.NewInternalErrorf(key, err.Error())
		}
		if err := appendListItem(v, item.Obj, uint64(0), opts.Predicate, s.codec, s.versioner); err != nil {
			return err
		}
	}

	// no continuation
	return s.versioner.UpdateList(listObj, uint64(0), "", nil)
}

func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, out runtime.Object, ignoreNotFound bool,
	precondtions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {

	trace := utiltrace.New(fmt.Sprintf("GuaranteedUpdate sqlite: %s", reflect.TypeOf(out).String()))
	defer trace.LogIfLong(500 * time.Millisecond)

	v, err := conversion.EnforcePtr(out)
	if err != nil {
		panic("unable to convert output object to pointer")
	}

	// stmt, err := s.client.Prepare(getSQL)
	// if err != nil {
	// 	return storage.NewInternalErrorf("key %v, transaction get prepare error %v", key, err.Error())
	// }
	// defer stmt.Close()

	rows, err := s.client.Query(fmt.Sprintf("SELECT revision,obj FROM keyval WHERE key='%s'", key))
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}
	defer rows.Close()

	oriData := dataModel{}
	//only fetch one item
	for rows.Next() {
		err := rows.Scan(&oriData.Revision, &oriData.Obj)
		if err != nil {
			return storage.NewInternalErrorf(key, err.Error())
		}
		break
	}

	oriObject := reflect.New(v.Type()).Interface().(runtime.Object)
	if oriObject != nil {
		if err := decode(s.codec, s.versioner, oriData.Obj, oriObject, 0); err != nil {
			return storage.NewInternalErrorf("decode origin data key %s error:%v", key, err.Error())
		}
	}

	ret, _, err := s.updateObj(oriObject, tryUpdate)
	if err != nil {
		klog.V(9).Infof("user update error :%v\r\n", err)
		return storage.NewInternalErrorf("key %s error:%v", key, err.Error())
	}

	newData := &dataModel{
		Key: key,
	}
	newData.Obj, err = runtime.Encode(s.codec, ret)
	if err != nil {
		return storage.NewInternalErrorf("key %v, object encode error %v", key, err.Error())
	}

	//data not change do nothing
	if bytes.Equal(newData.Obj, oriData.Obj) {
		return decode(s.codec, s.versioner, oriData.Obj, out, 0)
	}

	stmt, err := s.client.Prepare(createSQL)
	if err != nil {
		return storage.NewInternalErrorf("key %v, transaction prepare error %v", key, err.Error())
	}

	if _, err = stmt.Exec(newData.Key, newData.Revision, newData.Obj); err != nil {
		return storage.NewInternalErrorf("key %v, transaction update exec error %v", key, err.Error())
	}

	return decode(s.codec, s.versioner, newData.Obj, out, 0)
}

func (s *store) Count(key string) (int64, error) {
	return 0, nil
}

// growSlice takes a slice value and grows its capacity up
// to the maximum of the passed sizes or maxCapacity, whichever
// is smaller. Above maxCapacity decisions about allocation are left
// to the Go runtime on append. This allows a caller to make an
// educated guess about the potential size of the total list while
// still avoiding overly aggressive initial allocation. If sizes
// is empty maxCapacity will be used as the size to grow.
func growSlice(v reflect.Value, maxCapacity int, sizes ...int) {
	cap := v.Cap()
	max := cap
	for _, size := range sizes {
		if size > max {
			max = size
		}
	}
	if len(sizes) == 0 || max > maxCapacity {
		max = maxCapacity
	}
	if max <= cap {
		return
	}
	if v.Len() > 0 {
		extra := reflect.MakeSlice(v.Type(), 0, max)
		reflect.Copy(extra, v)
		v.Set(extra)
	} else {
		extra := reflect.MakeSlice(v.Type(), 0, max)
		v.Set(extra)
	}
}

func (s *store) userUpdate(input runtime.Object, userUpdate storage.UpdateFunc) (runtime.Object, uint64, error) {
	rv, err := s.versioner.ObjectResourceVersion(input)
	if err != nil {
		return nil, 0, fmt.Errorf("couldn't get resource version: %v", err)
	}
	respMeta := storage.ResponseMeta{
		TTL:             0,
		ResourceVersion: rv,
	}
	ret, ttl, err := userUpdate(input, respMeta)
	if err != nil {
		return nil, 0, err
	}

	return ret, *ttl, nil
}

// decode decodes value of bytes into object. It will also set the object resource version to rev.
// On success, objPtr would be set to the object.
func decode(codec runtime.Codec, versioner storage.Versioner, value []byte, objPtr runtime.Object, rev int64) error {
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		panic("unable to convert output object to pointer")
	}
	_, _, err := codec.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	versioner.UpdateObject(objPtr, uint64(rev))
	return nil
}

// appendListItem decodes and appends the object (if it passes filter) to v, which must be a slice.
func appendListItem(v reflect.Value, data []byte, rev uint64, pred storage.SelectionPredicate, codec runtime.Codec, versioner storage.Versioner) error {
	obj, _, err := codec.Decode(data, nil, reflect.New(v.Type().Elem()).Interface().(runtime.Object))
	if err != nil {
		return err
	}
	// being unable to set the version does not prevent the object from being extracted
	versioner.UpdateObject(obj, rev)
	if matched, err := pred.Matches(obj); err == nil && matched {
		v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
	}
	return nil
}

func (s *store) updateObj(obj runtime.Object, userUpdate storage.UpdateFunc) (runtime.Object, uint64, error) {
	ret, _, err := userUpdate(obj, storage.ResponseMeta{})
	if err != nil {
		return nil, 0, err
	}

	if err := s.versioner.PrepareObjectForStorage(ret); err != nil {
		return nil, 0, fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}
	return ret, 0, nil
}
