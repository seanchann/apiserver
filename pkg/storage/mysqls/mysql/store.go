/*

Copyright 2018 This Project Authors.

Author:  seanchann <seanchann@foxmail.com>

See docs/ for more information about the  project.

*/

package mysql

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apiserver/pkg/storage/mysqls"

	dbmysql "github.com/jinzhu/gorm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog"
	utiltrace "k8s.io/utils/trace"
)

const (
	//give a resourceversion with 1 if resource exist
	resourceVersion = 1
)

type store struct {
	client           *dbmysql.DB
	codec            runtime.Codec
	versioner        storage.Versioner
	storageVersion   string
	listDefaultLimit int
}

//dataModel
type dataModel struct {
	ID        int64  `gorm:"column:id;AUTO_INCREMENT;PRIMARY_KEY"`
	Name      string `gorm:"column:name;UNIQUE_INDEX:resource_idx"`
	Namespace string `gorm:"column:namespace;DEFAULT:'';UNIQUE_INDEX:resource_idx"`
	Revision  int64  `gorm:"column:revision"`
	Obj       []byte `gorm:"column:obj;NOT NULL;size:10240"`
}

const tableSQL = `CREATE TABLE IF NOT EXISTS %s (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  name varchar(255) DEFAULT NULL,
  namespace varchar(255) DEFAULT NULL,
	revision bigint(20) DEFAULT NULL,
  obj json NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY resource_idx (name,namespace)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
`

//New create a mysql store
func New(client *dbmysql.DB, codec runtime.Codec, version string, defaultLimit int) storage.Interface {
	return newStore(client, codec, version, defaultLimit)
}

const (
	tablecontextKey = iota
)

func newStore(client *dbmysql.DB, codec runtime.Codec, version string, defaultLimit int) *store {
	versioner := mysqls.APIObjectVersioner{}
	if len(version) == 0 {
		klog.Fatalln("need give a storage version for mysql backend")
	}
	if defaultLimit <= 0 {
		defaultLimit = 1000
	}

	return &store{
		client:           client,
		codec:            codec,
		versioner:        versioner,
		storageVersion:   version,
		listDefaultLimit: defaultLimit,
	}
}

func (s *store) Type() string {
	return string("mysql")
}

// Versioner implements storage.Interface.Versioner.
func (s *store) Versioner() storage.Versioner {
	return s.versioner
}

func (s *store) createTable(table string) {
	sql := fmt.Sprintf(tableSQL, table)
	s.client.Exec(sql)
}

func (s *store) Get(ctx context.Context, key string, opts storage.GetOptions, out runtime.Object) error {
	reqMeta := extractKey(ctx, key)
	kind := reqMeta.Kind
	resource := reqMeta.Resource
	if len(kind) == 0 || len(resource) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	query := fmt.Sprintf("name = ? ")
	queryArgs := []interface{}{resource}
	if len(reqMeta.Namespace) != 0 {
		query = fmt.Sprintf("name = ? AND namespace = ? ")
		queryArgs = []interface{}{resource, reqMeta.Namespace}
	}

	outData := &dataModel{}
	err := s.client.Table(kind).Where(query, queryArgs...).Limit(1).Find(outData).Error
	if err != nil {
		if dbmysql.IsRecordNotFoundError(err) {
			if opts.IgnoreNotFound {
				return runtime.SetZeroValue(out)
			}
			return storage.NewKeyNotFoundError(key, 0)
		}
		return storage.NewInternalErrorf(key, err.Error())
	}
	return decode(s.codec, s.versioner, outData.Obj, out, outData.Revision)
}

func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion should not be set on objects to be created")
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}

	reqMeta := extractKey(ctx, key)
	kind := reqMeta.Kind
	resource := reqMeta.Resource

	if len(kind) == 0 || len(resource) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	if !s.client.HasTable(kind) {
		s.createTable(kind)
		// if err := s.client.Table(kind).CreateTable(&dataModel{}).Error; err != nil {
		// 	return storage.NewInternalErrorf(key, err.Error())
		// }
	}

	data := &dataModel{
		Name:      resource,
		Namespace: reqMeta.Namespace,
	}
	var err error
	data.Obj, err = runtime.Encode(s.codec, obj)
	if err != nil {
		return storage.NewInternalErrorf("key %v, object encode error %v", key, err.Error())
	}

	err = s.client.Table(kind).Create(data).Error
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
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

	reqMeta := extractKey(ctx, key)
	kind := reqMeta.Kind
	resource := reqMeta.Resource
	if len(kind) == 0 || len(resource) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	query := fmt.Sprintf("name = ? ")
	queryArgs := []interface{}{resource}
	if len(reqMeta.Namespace) != 0 {
		query = fmt.Sprintf("name = ? AND namespace = ? ")
		queryArgs = []interface{}{resource, reqMeta.Namespace}
	}

	outData := &dataModel{}
	err = s.client.Table(kind).Where(query, queryArgs...).Limit(1).Find(outData).Error
	if err != nil {
		if dbmysql.IsRecordNotFoundError(err) {
			return storage.NewKeyNotFoundError(key, 0)
		}
		return storage.NewInternalErrorf(key, err.Error())
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

	query = fmt.Sprintf("id = ? ")
	queryArgs = []interface{}{outData.ID}
	err = s.client.Table(kind).Where(query, queryArgs...).Delete(dataModel{}).Error
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	return nil
}

// GuaranteedUpdate implements storage.Interface.GuaranteedUpdate.
func (s *store) GuaranteedUpdate(
	ctx context.Context, key string, out runtime.Object, ignoreNotFound bool,
	preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	trace := utiltrace.New(fmt.Sprintf("GuaranteedUpdate etcd3: %s", reflect.TypeOf(out).String()))
	defer trace.LogIfLong(500 * time.Millisecond)

	v, err := conversion.EnforcePtr(out)
	if err != nil {
		panic("unable to convert output object to pointer")
	}

	reqMeta := extractKey(ctx, key)
	kind := reqMeta.Kind
	resource := reqMeta.Resource
	if len(kind) == 0 || len(resource) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	if !s.client.HasTable(kind) {
		// if err := s.client.Table(kind).CreateTable(&dataModel{}).Error; err != nil {
		// 	return storage.NewInternalErrorf(key, err.Error())
		// }
		s.createTable(kind)
	}

	query := fmt.Sprintf("name = ? ")
	queryArgs := []interface{}{resource}
	if len(reqMeta.Namespace) != 0 {
		query = fmt.Sprintf("name = ? AND namespace = ? ")
		queryArgs = []interface{}{resource, reqMeta.Namespace}
	}

	oriData := &dataModel{}
	dbhandle := s.client.Table(kind).Where(query, queryArgs...)
	err = dbhandle.Limit(1).Find(oriData).Error
	if err != nil {
		if dbmysql.IsRecordNotFoundError(err) {
			if !ignoreNotFound {
				return storage.NewKeyNotFoundError(key, 0)
			}
		} else {
			return storage.NewInternalErrorf(key, err.Error())
		}
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
		Name:      resource,
		Namespace: reqMeta.Namespace,
	}
	newData.Obj, err = runtime.Encode(s.codec, ret)
	if err != nil {
		return storage.NewInternalErrorf("key %v, object encode error %v", key, err.Error())
	}

	//data not change do nothing
	if bytes.Equal(newData.Obj, oriData.Obj) {
		return decode(s.codec, s.versioner, oriData.Obj, out, 0)
	}

	updateOutData := &dataModel{}
	if err := dbhandle.
		Assign(map[string]interface{}{"name": newData.Name, "namespace": newData.Namespace, "obj": newData.Obj}).
		FirstOrCreate(updateOutData).Error; err != nil {
		return storage.NewInternalErrorf("key %v, object encode error %v", key, err.Error())
	}

	return decode(s.codec, s.versioner, updateOutData.Obj, out, 0)
}

func (s *store) GetToList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	v, err := conversion.EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		panic("need ptr to slice")
	}

	resMeta := extractKey(ctx, key)
	kind := resMeta.Kind
	if len(kind) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	klog.Infof("Call GetToList key %v pred %v ctx %v", key, opts.Predicate, ctx)

	data := []dataModel{}
	dbHandle := s.client.Table(kind)
	dbHandle = selectionWithFields(dbHandle, opts.Predicate, true)
	if err := dbHandle.Limit(1).Find(&data).Error; err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	if len(data) > 0 {
		if err := appendListItem(v, data[0].Obj, uint64(0), opts.Predicate, s.codec, s.versioner); err != nil {
			return err
		}
	}

	// update version with cluster level revision
	return s.versioner.UpdateList(listObj, uint64(0), "", nil)
}

type continueToken struct {
	Start uint64 `json:"start"`
	Total uint64 `json:"total"`
}

// parseFrom transforms an encoded predicate from into a versioned struct.
// TODO: return a typed error that instructs clients that they must relist
func decodeContinue(continueValue string) (skip uint64, err error) {
	data, err := base64.RawURLEncoding.DecodeString(continueValue)
	if err != nil {
		return 0, fmt.Errorf("continue key is not valid: %v", err)
	}
	var c continueToken
	if err := json.Unmarshal(data, &c); err != nil {
		return 0, fmt.Errorf("continue key is not valid: %v", err)
	}

	return c.Start, nil
}

// encodeContinue returns a string representing the encoded continuation of the current query.
func encodeContinue(start, total uint64, resourceVersion int64) (string, error) {
	out, err := json.Marshal(&continueToken{Start: start, Total: total})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(out), nil
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

	reqMeta := extracListKey(ctx, key)
	kind := reqMeta.Kind
	if len(kind) == 0 {
		return storage.NewKeyNotFoundError(key, 0)
	}

	if !s.client.HasTable(kind) {
		s.createTable(kind)
	}

	data := []dataModel{}
	dbHandle := s.client.Table(kind).Model(&data).Order("id")

	//check namesapce
	if len(reqMeta.Namespace) != 0 {
		query := fmt.Sprintf("namespace = ?")
		queryArgs := []interface{}{reqMeta.Namespace}
		dbHandle = dbHandle.Where(query, queryArgs...)
	}
	dbHandle = selectionWithFields(dbHandle, opts.Predicate, true)

	//first get list count by selection
	var listCount uint64
	err = dbHandle.Count(&listCount).Error
	if err != nil {
		return storage.NewInternalErrorf("key %v, query count error %v", key, err)
	}
	klog.V(3).Infof("find current list count %v", listCount)

	var skip uint64
	var nextSkip uint64
	var limit uint64

	if len(opts.Predicate.Continue) > 0 {
		//continue
		skip, err = decodeContinue(opts.Predicate.Continue)
		if err != nil {
			return apierrors.NewBadRequest(fmt.Sprintf("invalid continue token: %v", err))
		}
	}

	if opts.Predicate.Limit > 0 {
		//first resource
		limit = uint64(opts.Predicate.Limit)
	} else {
		limit = uint64(s.listDefaultLimit)
	}

	// set the appropriate clientv3 options to filter the returned data set
	if limit > 0 {
		dbHandle = dbHandle.Limit(limit)
	}

	if skip > 0 {
		dbHandle = dbHandle.Offset(skip)
	}

	klog.V(3).Infof("input pred %+v current limit %v skip %v", opts.Predicate, limit, skip)
	if err := dbHandle.Find(&data).Error; err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	if !(uint64(limit) >= listCount) {
		nextSkip = skip + uint64(limit)
	}
	growSlice(v, 2048, int(limit))

	for _, dataVal := range data {
		if err := appendListItem(v, dataVal.Obj, uint64(0), opts.Predicate, s.codec, s.versioner); err != nil {
			return err
		}
	}

	next, err := encodeContinue(nextSkip, listCount, 0)
	if err != nil {
		return storage.NewInternalErrorf(key, err.Error())
	}

	if nextSkip != 0 {
		// continuation
		c := int64(listCount - skip - limit)
		return s.versioner.UpdateList(listObj, uint64(0), next, &c)
	}
	// no continuation
	return s.versioner.UpdateList(listObj, uint64(0), next, nil)
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

//Count interface count
func (s *store) Count(key string) (int64, error) {
	resMeta := extractKey(nil, key)
	kind := resMeta.Kind
	if len(kind) == 0 {
		return 0, storage.NewKeyNotFoundError(key, 0)
	}
	klog.Infof("call count with key %v kind %v", key, kind)

	if !s.client.HasTable(kind) {
		s.createTable(kind)
	}

	var count uint64

	err := s.client.Table(kind).Count(&count).Error
	if err != nil {

		return 0, storage.NewInternalErrorf("key %v, query count error %v", key, err)
	}

	return int64(count), nil
}

func (s *store) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, storage.NewInternalError(fmt.Sprintf("the backend of mysql not support watch"))
}

func (s *store) WatchList(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, storage.NewInternalError(fmt.Sprintf("the backend of mysql not support watch"))
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
