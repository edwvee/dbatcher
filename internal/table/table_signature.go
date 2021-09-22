package table

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidTable  = errors.New("invalid table")
	ErrInvalidFields = errors.New("invalid  fields")
)

type TableSignature struct {
	tableName string
	fields    string
}

func NewTableSignature(tableName, fields string) TableSignature {
	return TableSignature{
		tableName: tableName,
		fields:    fields,
	}
}

func (t TableSignature) Validate() error {
	//TODO: more complex validate
	if t.tableName == "" {
		return ErrInvalidTable
	}
	if t.fields == "" {
		return ErrInvalidFields
	}

	return nil
}

func (t TableSignature) GetKey() string {
	return fmt.Sprintf("%s|%s", t.tableName, t.fields)
}
