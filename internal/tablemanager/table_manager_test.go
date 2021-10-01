package tablemanager

import (
	"reflect"
	"testing"

	"github.com/edwvee/dbatcher/internal/table"
)

func TestNewTableManager(t *testing.T) {
	tm := NewTableManager(
		&defaultTestTableSignature, defaultTestTableManagerConfig,
		defaultTestInserters,
	)
	tmExpected := &TableManager{
		table:       table.NewTable(defaultTestTableSignature),
		rowsJsons:   []byte{},
		inserters:   defaultTestInserters,
		maxRows:     int64(defaultTestTableManagerConfig.MaxRows),
		timeoutMs:   defaultTestTableManagerConfig.TimeoutMs,
		sendChannel: make(chan struct{}, 1),
		stopChannel: make(chan struct{}),
	}
	if !reflect.DeepEqual(*tm.table, *tmExpected.table) {
		t.Fatalf("table managers got different tables: want %v, got %v", *tm.table, *tmExpected.table)
	}
	tm.table = nil
	tmExpected.table = nil
	if tm.sendChannel == nil {
		t.Fatal("table manager got nil sendChannel")
	}
	tm.sendChannel = nil
	tmExpected.sendChannel = nil
	if tm.stopChannel == nil {
		t.Fatal("table manager got nil stopChannel")
	}
	tm.stopChannel = nil
	tmExpected.stopChannel = nil
	if !reflect.DeepEqual(tm, tmExpected) {
		t.Fatalf("want %v, got %v", tm, tmExpected)
	}
}
