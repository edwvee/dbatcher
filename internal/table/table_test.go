package table

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func getTestTable() *Table {
	ts := NewSignature("test_table", "field_1, field_2, field_3")
	return NewTable(ts)
}

func TestTableSignatureGetKey(t *testing.T) {
	name := "database.table"
	fields := "field1,field2"
	ts := NewSignature(name, fields)
	wantKey := "database.table|field1,field2"
	if gotKey := ts.GetKey(); wantKey != gotKey {
		t.Errorf("TableSignature GetKey: want %s, got %s", wantKey, gotKey)
	}
}

func TestAppendRowsFullPositive(t *testing.T) {
	table := getTestTable()

	values := [][]interface{}{
		{"43434", json.Number(strconv.Itoa(54)), "ererr"},
		{"gfhfdh", json.Number(strconv.Itoa(5864)), "ghjkgjfg"},
	}
	rowsJSON, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}

	err = table.AppendRows(rowsJSON)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	expectedLen := 3 * 2
	if len(table.data) != int(expectedLen) {
		t.Errorf("Wrong data length: want %d, got %d", expectedLen, len(table.data))
	}
	if rowsLen := table.GetRowsLen(); rowsLen != len(values) {
		t.Errorf("wrong rows len: expected %d, got %d", len(values), rowsLen)
	}

	expectedValues := []interface{}{}
	for _, arr := range values {
		expectedValues = append(expectedValues, arr...)
	}
	if !reflect.DeepEqual(table.data, expectedValues) {
		t.Errorf("table.data and encoded values are not equal")
	}

	for row, i := table.GetNextRow(), 0; row != nil; row, i = table.GetNextRow(), i+1 {
		if !reflect.DeepEqual(row, values[i]) {
			t.Errorf("row not equal after append: got %v, expected %v, i %d", row, values[i], i)
		}
	}
	if table.dataPos != 0 {
		t.Error("table.dataPos should be 0 after iterating over rows")
	}

	table.Free()
	if table.data != nil {
		t.Error("after free table data should be nil")
	}
}

func TestAppendRowsFullNegative(t *testing.T) {
	table := getTestTable()

	values := [][]interface{}{
		{"43434", json.Number(strconv.Itoa(54)), "ererr"},
		{"gfhfdh", "ghjkgjfg"},
	}
	rowsJSON, err := json.Marshal(values)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	err = table.AppendRows(rowsJSON)
	if !errors.Is(err, ErrWrongRowLen) {
		t.Fatal(err)
	}
	if len(table.data) != 0 {
		t.Error("negative append shouldn't add rows to table")
	}

	expectedLen := 0
	if len(table.data) != int(expectedLen) {
		t.Errorf("Wrong data length: want %d, got %d", expectedLen, len(table.data))
	}
	if rowsLen := table.GetRowsLen(); rowsLen != 0 {
		t.Errorf("wrong rows len: expected %d, got %d", 0, rowsLen)
	}

	values = [][]interface{}{
		{"43434", json.Number(strconv.Itoa(54)), "ererr"},
		{"gfhfdh", "ghjkgjfg"},
	}
	rowsJSON, err = json.Marshal(values)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	rowsJSON[0] = '{'

	err = table.AppendRows(rowsJSON)
	if err == nil || !strings.Contains(err.Error(), "json parsing") {
		t.Error("should be error about parsing json")
	}
}

func TestObvious(t *testing.T) {
	name := "database.table"
	fields := "field1,field2"
	ts := NewSignature(name, fields)
	tbl := NewTable(ts)
	if nameFromTbl := tbl.GetTableName(); nameFromTbl != name {
		t.Errorf("get table name: want %s, got %s", name, nameFromTbl)
	}
	if fieldsFromTbl := tbl.GetFields(); fieldsFromTbl != fields {
		t.Errorf("get fields: want %s, got %s", fields, fieldsFromTbl)
	}
}
