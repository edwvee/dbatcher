package table

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

var (
	ErrEmptyTableName           = errors.New("empty table name")
	ErrEmptyTableNamePart       = errors.New("empty table name part")
	ErrTableNameInvalidBacktick = errors.New("table name has invalid backtick")
	ErrEmptyFields              = errors.New("empty  fields")
	ErrEmptyField               = errors.New("fields contain empty field (check commas)")
	ErrFieldInvalidBacktick     = errors.New("one of the fields has invalid backtick")
)

type TableSignature struct {
	tableName string
	fields    string
}

func NewTableSignature(tableName, fields string) TableSignature {
	return TableSignature{
		tableName: tableName,
		fields:    strings.Replace(fields, " ", "", -1),
	}
}

func (ts TableSignature) Validate() error {
	if err := ts.validateTableName(); err != nil {
		return err
	}
	return ts.validateFields()
}

func (ts TableSignature) validateTableName() error {
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

func (ts TableSignature) validateFields() error {
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

func (ts TableSignature) GetKey() string {
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
