package table

import (
	"encoding/json"
	"reflect"
	"testing"
)

func getTestTable() *Table {
	ts := NewTableSignature("test_table", "field_1, field_2, field_3")
	return NewTable(ts)
}

func TestTableSignatureGetKey(t *testing.T) {
	name := "database.table"
	fields := "field1,field2"
	ts := NewTableSignature(name, fields)
	wantKey := "database.table|field1,field2"
	if gotKey := ts.GetKey(); wantKey != gotKey {
		t.Errorf("TableSignature GetKey: want %s, got %s", wantKey, gotKey)
	}
}

func TestAppendRowsPositive(t *testing.T) {
	table := getTestTable()

	//float64 cause json has no int
	values := [][]interface{}{{"43434", float64(54), "ererr"}, {"gfhfdh", float64(5864), "ghjkgjfg"}}
	rowsJson, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}

	err = table.AppendRows(rowsJson)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedLen := 3 * 2
	if len(table.data) != int(expectedLen) {
		t.Errorf("Wrong data length: want %d, got %d", expectedLen, len(table.data))
	}

	expectedValues := []interface{}{}
	for _, arr := range values {
		expectedValues = append(expectedValues, arr...)
	}
	if !reflect.DeepEqual(table.data, expectedValues) {
		t.Errorf("table.data and encoded values are not equal")
	}
}

func TestObvious(t *testing.T) {
	name := "database.table"
	fields := "field1,field2"
	ts := NewTableSignature(name, fields)
	tbl := NewTable(ts)
	if nameFromTbl := tbl.GetTableName(); nameFromTbl != name {
		t.Errorf("get table name: want %s, got %s", name, nameFromTbl)
	}
	if fieldsFromTbl := tbl.GetFields(); fieldsFromTbl != fields {
		t.Errorf("get fields: want %s, got %s", fields, fieldsFromTbl)
	}
}
