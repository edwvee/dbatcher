package tablemanager

import (
	"reflect"
	"testing"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
)

var defaultTestInserters = map[string]inserter.Inserter{
	"dummy": &inserter.DummyInserter{},
}
var defaultTestErrChan = make(chan error)
var defaultTestTableSignature = table.NewTableSignature("db.`table`", "field1, field2, field3")
var defaultTestTableManagerConfig = NewTableManagerConfig(1000, 100, false)

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

func TestStopTableManagers(t *testing.T) {
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
