/*

Copyright 2018 This Project Authors.

Author:  seanchann <seanchann@foxmail.com>

See docs/ for more information about the  project.

*/

package mysql

import (
	"context"
	"fmt"

	"strings"

	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog"
	"github.com/jinzhu/gorm"
)

//appendQuoteToField append quote into filed for every member。
// eg 'spec.test.name' ====> '"spec"."test"."name"'
func appendQuoteToField(input string) (field string) {
	members := strings.Split(input, ".")
	if len(members) > 0 {
		var quoteMember []string
		for _, v := range members {
			m := fmt.Sprintf("\"%s\"", v)
			quoteMember = append(quoteMember, m)
		}
		field = strings.Join(quoteMember, ".")
	}

	return
}

//Fields build gorm select condition by storage.SelectionPredicate
//selectionFeild contains what field will be select for query
func selectionWithFields(dbHandle *gorm.DB, p storage.SelectionPredicate, isCount bool) *gorm.DB {
	if p.Field == nil || (p.Field != nil && p.Field.Empty()) {
		return dbHandle
	}

	fieldsCondition := p.Field.Requirements()
	for _, v := range fieldsCondition {
		switch v.Operator {
		case selection.Equals:
			fallthrough
		case selection.DoubleEquals:
			query := fmt.Sprintf("JSON_CONTAINS(%s, '\"%s\"', '$.%s') = ?",
				"obj", v.Value, appendQuoteToField(v.Field))
			queryArgs := "1"

			dbHandle = dbHandle.Where(query, queryArgs)
		case selection.NotEquals:
			query := fmt.Sprintf("JSON_CONTAINS(%s, '\"%s\"', '$.%s') = ?",
				"obj", v.Value, appendQuoteToField(v.Field))
			queryArgs := "0"

			dbHandle = dbHandle.Where(query, queryArgs)
		//TODO: this can't be support if have specific Requirements
		case selection.In:
			fallthrough
		case selection.NotIn:
			klog.Warningf("not support in and not in")
		case selection.DoesNotExist:
			fallthrough
		case selection.Exists:
			//only search obj filed for this operator,ignore count
			if isCount {
				continue
			}
			query := fmt.Sprintf("JSON_CONTAINS_PATH(%s, 'one', '$.%s') = ?",
				"obj", appendQuoteToField(v.Field))
			queryArgs := "1"
			if selection.DoesNotExist == v.Operator {
				queryArgs = "0"
			}
			dbHandle = dbHandle.Where(query, queryArgs)
		}
	}

	return dbHandle
}

type requestMeta struct {
	Namespace string
	Kind      string
	Resource  string
}

func extractKey(ctx context.Context, key string) *requestMeta {
	reqMeta := &requestMeta{}

	keySlice := strings.Split(key, "/")
	if len(keySlice) == 4 {
		reqMeta.Kind = keySlice[1]
		reqMeta.Namespace = keySlice[2]
		reqMeta.Resource = keySlice[3]
	} else if len(keySlice) == 3 {
		reqMeta.Kind = keySlice[1]
		reqMeta.Resource = keySlice[2]
	} else if len(keySlice) == 2 {
		reqMeta.Kind = keySlice[1]
	}

	klog.V(4).Infof("extract key %v out reqmeta %#v", key, reqMeta)

	return reqMeta
}

func extracListKey(ctx context.Context, key string) *requestMeta {
	reqMeta := &requestMeta{}

	keySlice := strings.Split(key, "/")
	if len(keySlice) == 3 {
		reqMeta.Kind = keySlice[1]
		reqMeta.Namespace = keySlice[2]
	} else if len(keySlice) == 2 {
		reqMeta.Kind = keySlice[1]
	} 

	klog.V(4).Infof("extract key %v out reqmeta %#v", key, reqMeta)

	return reqMeta	
}