package table

import (
	"bytes"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var ErrWrongRowLen = errors.New("")

type Table struct {
	TableSignature

	data    []interface{}
	dataPos int
	rowLen  int
}

func NewTable(ts TableSignature) *Table {
	data := tableDataPool.Get().([]interface{})[:0]
	rowLen := strings.Count(ts.fields, ",") + 1
	return &Table{
		TableSignature: ts,
		data:           data,
		rowLen:         rowLen,
	}
}

func (t *Table) Free() {
	if len(t.data) <= maxTableDataSize {
		tableDataPool.Put(t.data) //lint:ignore SA6002 it's slice
	}
	t.data = nil
}

func (t *Table) AppendRows(rowsJson []byte) error {
	var target [][]interface{}
	decoder := jsoniter.NewDecoder(bytes.NewReader(rowsJson))
	decoder.UseNumber()
	err := decoder.Decode(&target)
	if err != nil {
		return errors.Wrap(err, "table: append rows: json parsing:")
	}
	for _, el := range target {
		if len(el) != t.rowLen {
			return t.wrongLengthErr(el)
		}
	}
	for _, el := range target {
		t.data = append(t.data, el...)
	}

	return nil
}

func (t Table) GetRowsLen() int {
	return len(t.data) / t.rowLen
}

func (t Table) wrongLengthErr(el []interface{}) error {
	return errors.Wrapf(
		ErrWrongRowLen,
		"wrong row length: need %d, got %d, row %v",
		t.rowLen, len(el), el,
	)
}

func (t Table) GetTableName() string {
	return t.tableName
}

func (t Table) GetFields() string {
	return t.fields
}

func (t *Table) GetNextRow() []interface{} {
	if len(t.data) <= t.dataPos {
		t.dataPos = 0
		return nil
	}
	nextPos := t.dataPos + t.rowLen
	row := t.data[t.dataPos:nextPos]
	t.dataPos = nextPos

	return row
}
