package inserter

import (
	"bytes"
	"os"
	"testing"

	"github.com/edwvee/dbatcher/internal/table"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

func TestNewInsertErrorLoggerFromConfig(t *testing.T) {
	config := InsertErrorLoggerConfig{"test_path.log", true}
	logger, err := NewInsertErrorLoggerFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	err = logger.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(config.Path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInsertErrorLoggerLog(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewInsertErrorLogger(buf, false)
	tableName := "database.table"
	fields := "field1,field2,field3"
	table := table.NewTable(table.NewSignature(tableName, fields))
	data := []byte("[[\"test\",0,2.4],[\"test_test\",17,4.4]]")
	table.AppendRows(data)
	errorMessage := "test error"
	err := logger.Log(errors.New(errorMessage), table)
	if err != nil {
		t.Fatal(err)
	}

	var result insertErrorLoggerData
	resultData := buf.Bytes()
	err = jsoniter.Unmarshal(resultData, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.Error != errorMessage {
		t.Errorf("wrong error message: got %s, want %s", result.Error, errorMessage)
	}
	if result.Table != tableName {
		t.Errorf("wrong table name: got %s, want %s", result.Table, tableName)
	}
	if result.Fields != fields {
		t.Errorf("wrong table fields: got %s, want %s", result.Fields, fields)
	}
	marshalledRows, err := jsoniter.Marshal(result.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if string(marshalledRows) != string(data) {
		t.Errorf("wrong rows after json marshalling: got %s, want %s", marshalledRows, data)
	}
}
