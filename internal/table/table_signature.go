package table

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

var (
	//ErrEmptyTableName means table's name is empty
	ErrEmptyTableName = errors.New("empty table name")
	//ErrEmptyTableNamePart means there are empty table's name's part. E.g., "db..name", "db.", "db.name.""
	ErrEmptyTableNamePart = errors.New("empty table name part")
	//ErrTableNameInvalidBacktick means there is no closing/opening backtick it table's name
	ErrTableNameInvalidBacktick = errors.New("table name has invalid backtick")
	//ErrEmptyFields means fields are empty
	ErrEmptyFields = errors.New("empty  fields")
	//ErrEmptyField one of fields is empty ("field1,,field2")
	ErrEmptyField = errors.New("fields contain empty field (check commas)")
	//ErrFieldInvalidBacktick means there is no closing/opening backtick it fields
	ErrFieldInvalidBacktick = errors.New("one of the fields has invalid backtick")
)

//Signature is table's name and fields
type Signature struct {
	tableName string
	fields    string
}

//NewSignature creates new TableSignature from name and fields
func NewSignature(tableName, fields string) Signature {
	return Signature{
		tableName: tableName,
		fields:    strings.Replace(fields, " ", "", -1),
	}
}

//Validate validates table signature
func (ts Signature) Validate() error {
	if err := ts.validateTableName(); err != nil {
		return err
	}
	return ts.validateFields()
}

func (ts Signature) validateTableName() error {
	if ts.tableName == "" {
		return ErrEmptyTableName
	}
	parts := strings.Split(ts.tableName, ".")
	for _, part := range parts {
		if part == "" {
			return ErrEmptyTableNamePart
		}
		if !validateBackticks(part) {
			return ErrTableNameInvalidBacktick
		}
	}

	return nil
}

func (ts Signature) validateFields() error {
	if ts.fields == "" {
		return ErrEmptyFields
	}
	parts := strings.Split(ts.fields, ",")
	for _, part := range parts {
		if part == "" {
			return ErrEmptyField
		}
		if !validateBackticks(part) {
			return ErrFieldInvalidBacktick
		}
	}

	return nil
}

//GetKey returns a key from table name and fields to identify table
func (ts Signature) GetKey() string {
	return fmt.Sprintf("%s|%s", ts.tableName, ts.fields)
}

func validateBackticks(str string) bool {
	if str[0] == '`' && str[len(str)-1] != '`' {
		return false
	}
	if str[len(str)-1] == '`' && str[0] != '`' {
		return false
	}

	return true
}
