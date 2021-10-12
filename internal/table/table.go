package table

import (
	"bytes"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

//ErrWrongRowLen - when row elements count doesn't match table's fields count
var ErrWrongRowLen = errors.New("")

//Table is an abstract table that has name, fields and it's data (rows)
type Table struct {
	Signature

	data    []interface{}
	dataPos int
	rowLen  int
}

//NewTable creates new table by signature
func NewTable(ts Signature) *Table {
	data := tableDataPool.Get().([]interface{})[:0]
	rowLen := strings.Count(ts.fields, ",") + 1
	return &Table{
		Signature: ts,
		data:      data,
		rowLen:    rowLen,
	}
}

//Free frees table's resources
func (t *Table) Free() {
	if len(t.data) <= maxTableDataSize {
		tableDataPool.Put(t.data) //lint:ignore SA6002 it's slice
	}
	t.data = nil
}

//AppendRows parses rowsJSON as [][]interface{}, validates
//and appends to table's inner data buffer
func (t *Table) AppendRows(rowsJSON []byte) error {
	var target [][]interface{}
	decoder := jsoniter.NewDecoder(bytes.NewReader(rowsJSON))
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

//GetRowsLen returns count of table's rows
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

//GetTableName returns table's name
func (t Table) GetTableName() string {
	return t.tableName
}

//GetFields returns table's fields
func (t Table) GetFields() string {
	return t.fields
}

//GetNextRow iterates over table's data buffer
//and returns each row. When the end of data is reached
//returns nil and reset's inner iteration position.
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

func (t *Table) GetRawData() []interface{} {
	return t.data
}
