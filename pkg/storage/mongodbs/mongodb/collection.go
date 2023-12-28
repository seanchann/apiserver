/********************************************************************
* Copyright (c) 2008 - 2024. seanchann <seanchann.zhou@gmail.com>
* All rights reserved.
*
* PROPRIETARY RIGHTS of the following material in either
* electronic or paper format pertain to sean.
* All manufacturing, reproduction, use, and sales involved with
* this subject MUST conform to the license agreement signed
* with sean.
*******************************************************************/

package mongodb

import (
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/mongodbs/client"

	mgo "gopkg.in/mgo.v2"
)

type Collection struct {
	name        string
	database    string
	keyIndex    []string
	expireIndex []string
}

const (
	//if set expire index. if index expire then will be after this period to remove doc
	expirePeriod = time.Duration(0) * time.Second
	//this index is our convention  for runtime object
	uidIndex    = "uid"
	keyIndex    = "key"
	expireIndex = "ttl"
)

func GetCollection(dbName string, sess *mgo.Session, obj runtime.Object) (*Collection, error) {
	collection := GetObjKind(obj)
	if len(collection) == 0 {
		return nil, storage.NewInternalError(fmt.Sprintf("object(%v) not have kind", reflect.TypeOf(obj)))
	}

	c := &Collection{
		name:        collection,
		database:    dbName,
		keyIndex:    []string{uidIndex, keyIndex},
		expireIndex: []string{expireIndex},
	}

	//ensure index
	err := c.CreateIndex(c.GetRequestMeta(sess))
	if err != nil {
		return nil, storage.NewInternalError(err.Error())
	}

	return c, nil
}

// CreateIndex by runtime object
func (c *Collection) CreateIndex(meta *client.RequestMeta) error {
	err := client.MongoEnsureIndex(meta, c.keyIndex)
	if err != nil {
		return err
	}

	err = client.MongoEnsureIndexWithExpire(meta, c.expireIndex, expirePeriod)
	if err != nil {
		return err
	}
	return nil
}

func (c *Collection) GetRequestMeta(sess *mgo.Session) *client.RequestMeta {
	return &client.RequestMeta{
		DBName:     c.database,
		Collection: c.name,
		Sess:       sess,
	}
}
