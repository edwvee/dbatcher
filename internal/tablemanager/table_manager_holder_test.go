package tablemanager

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
)

func TestNewTableManagerHolder(t *testing.T) {
	tmh := NewTableManagerHolder(defaultTestErrChan, defaultTestInserters)
	if !reflect.DeepEqual(tmh.inserters, defaultTestInserters) {
		t.Errorf("unequal inserters: %v, %v", tmh.inserters, defaultTestInserters)
	}
	if tmh.managers == nil {
		t.Errorf("managers shouldn't be nill")
	}
	if len(tmh.managers) != 0 {
		t.Errorf("len(managers) should be zero")
	}
	if tmh.lastManagerVisit == nil {
		t.Errorf("lastManagerVisit shouldn't be nill")
	}
	if len(tmh.lastManagerVisit) != 0 {
		t.Errorf("len(lastManagerVisit) should be zero")
	}
}

func TestGetTableManager(t *testing.T) {
	tmc := defaultTestTableManagerConfig
	tmh := NewTableManagerHolder(defaultTestErrChan, defaultTestInserters)
	tm := tmh.getTableManager(&defaultTestTableSignature, tmc)
	if tm == nil {
		t.Error("got nil table manager")
	}
	key := defaultTestTableSignature.GetKey()
	if _, ok := tmh.managers[key]; !ok {
		t.Errorf("table manager not present in map after return")
	}
	if _, ok := tmh.lastManagerVisit[key]; !ok {
		t.Errorf("didn't add last manager visit")
	}
	//lint:ignore SA5011 insufficient: it's test
	if !reflect.DeepEqual(tm.inserters, defaultTestInserters) {
		t.Error("didn't pass inserters")
	}
	//lint:ignore SA5011 insufficient: tm isn't nill
	if tm.maxRows != tmc.MaxRows || tm.timeoutMs != tmc.TimeoutMs {
		t.Error("manager didn't receive config correctly")
	}

	tmc.TimeoutMs = 100
	tmc.MaxRows = 1000
	tmNew := tmh.getTableManager(&defaultTestTableSignature, tmc)
	if tm != tmNew {
		t.Error("should be same table managers with same table signature")
	}
	if tmNew.maxRows != tmc.MaxRows || tmNew.timeoutMs != tmc.TimeoutMs {
		t.Error("didn't update table manager config")
	}
}

func TestStopUnusedTableManagers(t *testing.T) {
	tmc := defaultTestTableManagerConfig
	tmh := NewTableManagerHolder(defaultTestErrChan, defaultTestInserters)
	tmh.getTableManager(&defaultTestTableSignature, tmc)
	if len(tmh.managers) == 0 {
		t.Errorf("should present table manager in map")
	}
	if len(tmh.lastManagerVisit) == 0 {
		t.Errorf("should present last manager visit")
	}
	stopUnusedManagersInterval = time.Second
	tmh.StopUnusedManagers()
	time.Sleep(stopUnusedManagersInterval + time.Second)
	tmh.managersMut.Lock()
	if len(tmh.managers) != 0 {
		t.Errorf("shouldn't present any table manager in map")
	}
	if len(tmh.lastManagerVisit) != 0 {
		t.Errorf("shouldn't present any last manager visit")
	}
	tmh.managersMut.Unlock()
}

func TestStopTableManagersPositive(t *testing.T) {
	tmc := defaultTestTableManagerConfig
	si := &selfSliceInserter{}
	si.Init(inserter.Config{})
	inserters := map[string]inserter.Inserter{"self slice inserter": si}
	tmh := NewTableManagerHolder(defaultTestErrChan, inserters)
	tmh.getTableManager(&defaultTestTableSignature, tmc)

	const managersSize = 10
	for i := 0; i < managersSize; i++ {
		ts := table.NewTableSignature(fmt.Sprintf("table%d", i), "field1")
		err := tmh.Append(&ts, defaultTestTableManagerConfig, false, []byte("[[1]]"))
		if err != nil {
			t.Fatal(err)
		}
		tmh.getTableManager(&ts, tmc)
	}
	if errs := tmh.StopTableManagers(); len(errs) != 0 {
		for _, err := range errs {
			t.Errorf("didn't stop manager, got error: %s", err.Error())
		}
	}
	data := si.TakeSlice()
	if len(data) != managersSize {
		t.Errorf("not all data was inserted")
	}
}

func TestStopTableManagersNegative(t *testing.T) {
	tmc := defaultTestTableManagerConfig
	inserters := map[string]inserter.Inserter{"first": &longSleepInserter{}, "second": &longSleepInserter{}}
	tmh := NewTableManagerHolder(defaultTestErrChan, inserters)
	tmh.getTableManager(&defaultTestTableSignature, tmc)

	const managersSize = 10
	for i := 0; i < managersSize; i++ {
		ts := table.NewTableSignature(fmt.Sprintf("table%d", i), "field1")
		err := tmh.Append(&ts, defaultTestTableManagerConfig, false, []byte("[[1]]"))
		if err != nil {
			t.Fatal(err)
		}
		tmh.getTableManager(&ts, tmc)
	}
	errs := tmh.StopTableManagers()
	if len(errs) != managersSize {
		t.Fatalf("should be %d errors, got %d", managersSize, len(errs))
	}
	for _, err := range errs {
		if !errors.Is(err, ErrTableManagerDidntStopInTime) {
			t.Fatalf("all errors should be ErrTableManagerDidntStopInTime, got: %v", err)
		}
	}
}
