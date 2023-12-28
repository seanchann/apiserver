/*

Copyright 2018 This Project Authors.

Author:  seanchann <seanchann@foxmail.com>

See docs/ for more information about the  project.

*/

package mysqls

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

func TestObjectVersioner(t *testing.T) {
	v := APIObjectVersioner{}
	if ver, err := v.ObjectResourceVersion(&storagetesting.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}}); err != nil || ver != 5 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	if ver, err := v.ObjectResourceVersion(&storagetesting.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "a"}}); err == nil || ver != 0 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	obj := &storagetesting.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "a"}}
	if err := v.UpdateObject(obj, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.ResourceVersion != "5" || obj.DeletionTimestamp != nil {
		t.Errorf("unexpected resource version: %#v", obj)
	}
}

func TestEtcdParseResourceVersion(t *testing.T) {
	testCases := []struct {
		Version       string
		ExpectVersion uint64
		Err           bool
	}{
		{Version: "", ExpectVersion: 0},
		{Version: "a", Err: true},
		{Version: " ", Err: true},
		{Version: "1", ExpectVersion: 1},
		{Version: "10", ExpectVersion: 10},
	}

	v := APIObjectVersioner{}
	testFuncs := []func(string) (uint64, error){
		v.ParseResourceVersion,
	}

	for _, testCase := range testCases {
		for i, f := range testFuncs {
			version, err := f(testCase.Version)
			switch {
			case testCase.Err && err == nil:
				t.Errorf("%s[%v]: unexpected non-error", testCase.Version, i)
			case testCase.Err && !storage.IsInvalidError(err):
				t.Errorf("%s[%v]: unexpected error: %v", testCase.Version, i, err)
			case !testCase.Err && err != nil:
				t.Errorf("%s[%v]: unexpected error: %v", testCase.Version, i, err)
			}
			if version != testCase.ExpectVersion {
				t.Errorf("%s[%v]: expected version %d but was %d", testCase.Version, i, testCase.ExpectVersion, version)
			}
		}
	}
}

func TestCompareResourceVersion(t *testing.T) {
	five := &storagetesting.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}}
	six := &storagetesting.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "6"}}

	versioner := APIObjectVersioner{}

	if e, a := -1, versioner.CompareResourceVersion(five, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 1, versioner.CompareResourceVersion(six, five); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 0, versioner.CompareResourceVersion(six, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
}
