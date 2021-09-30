package table

import (
	"errors"
	"testing"
)

func TestTableSignatureValidate(t *testing.T) {
	ts := NewTableSignature("", "")
	if err := ts.Validate(); !errors.Is(err, ErrEmptyTableName) {
		t.Errorf("when tableName is %s there should be ErrEmptyTableName", ts.tableName)
	}
	ts.tableName = "database."
	if err := ts.Validate(); !errors.Is(err, ErrEmptyTableNamePart) {
		t.Errorf("when tableName is %s there should be ErrEmptyTableNamePart", ts.tableName)
	}
	ts.tableName = "`database`.`table"
	if err := ts.Validate(); !errors.Is(err, ErrTableNameInvalidBacktick) {
		t.Errorf("when tableName is %s there should be ErrTableNameInvalidBacktick", ts.tableName)
	}
	ts.tableName = "database`.`table`"
	if err := ts.Validate(); !errors.Is(err, ErrTableNameInvalidBacktick) {
		t.Errorf("when tableName is %s there should be ErrTableNameInvalidBacktick", ts.tableName)
	}
	ts.tableName = "`database`.`table`"
	if err := ts.Validate(); !errors.Is(err, ErrEmptyFields) {
		t.Errorf("when tableName is %s and fields are %s there should be ErrEmptyFields", ts.tableName, ts.fields)
	}
	ts.fields = "field1,,field3"
	if err := ts.Validate(); !errors.Is(err, ErrEmptyField) {
		t.Errorf("when tableName is %s and fields are %s there should be ErrEmptyField", ts.tableName, ts.fields)
	}
	ts.fields = "`field1,field3"
	if err := ts.Validate(); !errors.Is(err, ErrFieldInvalidBacktick) {
		t.Errorf("when tableName is %s and fields are %s there should be ErrFieldInvalidBacktick", ts.tableName, ts.fields)
	}
	ts.fields = "field1`,field3"
	if err := ts.Validate(); !errors.Is(err, ErrFieldInvalidBacktick) {
		t.Errorf("when tableName is %s and fields are %s there should be ErrFieldInvalidBacktick", ts.tableName, ts.fields)
	}

	ts.fields = "field1,`field3`"
	if err := ts.Validate(); err != nil {
		t.Errorf("when tableName is %s and fields are %s TableSignature should be valid", ts.tableName, ts.fields)
	}
}
