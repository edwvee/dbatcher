package table

import (
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/pquerna/ffjson/ffjson"
)

var jData = []byte("[[4342,\"fdsfdf\",434,\"XTYPE\",\"fdsfdfdsfsdf/fsdfdsf/fsdfdsfsdfds\"],[4342,\"fdsfdf\",434,\"XTYPE\",\"fdsfdfdsfsdf/fsdfdsf/fsdfdsfsdfds\"],[4342,\"fdsfdf\",434,\"XTYPE\",\"fdsfdfdsfsdf/fsdfdsf/fsdfdsfsdfds\"]]")

func TestJson(t *testing.T) {
	var target [][]interface{}
	err := json.Unmarshal(jData, &target)
	if err != nil {
		t.Error(err)
	}
	res := []interface{}{}
	for _, el := range target {
		res = append(res, el...)
	}
	t.Logf("%v", res)
}

func TestJsonIter(t *testing.T) {
	var target [][]interface{}
	err := jsoniter.Unmarshal(jData, &target)
	if err != nil {
		t.Error(err)
	}
	res := []interface{}{}
	for _, el := range target {
		res = append(res, el...)
	}
	t.Logf("%v", res)
}

func TestFfjson(t *testing.T) {
	var target [][]interface{}
	err := ffjson.Unmarshal(jData, &target)
	if err != nil {
		t.Error(err)
	}
	res := []interface{}{}
	for _, el := range target {
		res = append(res, el...)
	}
	t.Logf("%v", res)
}

func BenchmarkJson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var target [][]interface{}
		err := json.Unmarshal(jData, &target)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkJsoniter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var target [][]interface{}
		err := jsoniter.Unmarshal(jData, &target)
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkFfjson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var target [][]interface{}
		err := ffjson.Unmarshal(jData, &target)
		if err != nil {
			b.Error(err)
		}
	}
}
