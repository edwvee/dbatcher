package tablemanager

import (
	"errors"
	"sync"
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

type selfSliceInserter struct {
	data    []interface{}
	dataMut sync.Mutex
}

func (si *selfSliceInserter) Init(c inserter.Config) error {
	si.data = []interface{}{}
	return nil
}

func (si *selfSliceInserter) Insert(t *table.Table) error {
	si.dataMut.Lock()
	for row := t.GetNextRow(); row != nil; row = t.GetNextRow() {
		si.data = append(si.data, row)
	}
	si.dataMut.Unlock()

	return nil
}

func (si *selfSliceInserter) TakeSlice() []interface{} {
	si.dataMut.Lock()
	defer si.dataMut.Unlock()
	res := si.data
	si.data = nil

	return res
}

type longSleepInserter struct{}

func (si *longSleepInserter) Init(c inserter.Config) error {
	return nil
}

func (si *longSleepInserter) Insert(t *table.Table) error {
	time.Sleep(time.Minute)

	return nil
}

type errorInserter struct{}

func (si *errorInserter) Init(c inserter.Config) error {
	return nil
}

func (si *errorInserter) Insert(t *table.Table) error {
	return errors.New("some error")
}
