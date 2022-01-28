package tablemanager

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
)

func TestNewTableManager(t *testing.T) {
	logger := inserter.NewInsertErrorLogger(nil, false)
	tm := NewTableManager(
		&defaultTestTableSignature, defaultTestTableManagerConfig,
		defaultTestInserters, logger,
	)
	tmExpected := &TableManager{
		table:             table.NewTable(defaultTestTableSignature),
		rowsJsons:         []byte{},
		inserters:         defaultTestInserters,
		insertErrorLogger: logger,
		maxRows:           int64(defaultTestTableManagerConfig.MaxRows),
		timeoutMs:         defaultTestTableManagerConfig.TimeoutMs,
		sendChannel:       make(chan struct{}, 1),
		stopChannel:       make(chan struct{}),
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

func TestShouldAppendWhenTooMuchRows(t *testing.T) {
	const maxRows = 1000
	tmc := NewConfig(100000000, maxRows, false)
	si := &selfSliceInserter{}
	si.Init(inserter.Config{})
	inserters := map[string]inserter.Inserter{"self slice inserter": si}
	logger := inserter.NewInsertErrorLogger(nil, false)
	tm := NewTableManager(&defaultTestTableSignature, tmc, inserters, logger)
	go tm.Run()
	for i := 0; i < maxRows; i++ {
		err := tm.AppendRowsToTable([]byte("[[1,2,3]]"))
		if err != nil {
			t.Fatal(err)
		}
	}
	//second is too long, but insert is async anyway
	time.Sleep(time.Second)
	data := si.TakeSlice()
	if len(data) != maxRows {
		t.Fatal("didn't insert rows")
	}
}

func TestShouldReturnMultiError(t *testing.T) {
	tmc := NewConfig(1000, 100, false)
	inserters := map[string]inserter.Inserter{"1": &errorInserter{}, "2": &errorInserter{}}
	logger := inserter.NewInsertErrorLogger(nil, false)
	tm := NewTableManager(&defaultTestTableSignature, tmc, inserters, logger)
	err := tm.AppendRowsToTable([]byte("[[1,2,3]]"))
	if err != nil {
		t.Fatal(err)
	}
	err = tm.insertConcurrently(tm.getTableAndMakeNew())
	if err == nil {
		t.Fatal("err should be not nil")
	}
	errMessage := err.Error()
	if !strings.Contains(errMessage, "some error") {
		t.Fatal("should contain \"some error\"")
	}
	errMessage = strings.Replace(errMessage, "some error", "", 1)
	if !strings.Contains(errMessage, "some error") {
		t.Fatal("should contain two \"some error\"")
	}
}

func TestMultiInserters(t *testing.T) {
	const maxRows = 10
	tmc := NewConfig(1000, maxRows, false)
	inserters := map[string]inserter.Inserter{}
	for i := 0; i < 1000; i++ {
		si := &selfSliceInserter{}
		si.Init(inserter.Config{})
		inserters[strconv.Itoa(i)] = si
	}
	logger := inserter.NewInsertErrorLogger(nil, false)
	tm := NewTableManager(&defaultTestTableSignature, tmc, inserters, logger)
	go tm.Run()
	for i := 0; i < maxRows; i++ {
		err := tm.AppendRowsToTable([]byte("[[1,2,3]]"))
		if err != nil {
			t.Fatal(err)
		}
	}
	//second is too long, but insert is async anyway
	time.Sleep(time.Second)
	for _, inserter := range inserters {
		si, ok := inserter.(*selfSliceInserter)
		if !ok {
			t.Fail()
		}
		data := si.TakeSlice()
		if len(data) != maxRows {
			t.Errorf("should be %d rows in inserter, got %d", maxRows, len(data))
		}
	}
}
