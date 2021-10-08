package tablemanager

import (
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
)

//TableManager is responsible for a table and calling inserters on it.
//Serves as frontend to a table
type TableManager struct {
	table     *table.Table
	tableMut  sync.Mutex
	rowsJsons []byte

	maxRows     int64
	inserters   map[string]inserter.Inserter
	timeoutMs   int64
	sendChannel chan struct{}
	stopChannel chan struct{}
}

//NewTableManager returns configured table manager
func NewTableManager(ts *table.Signature, config Config, inserters map[string]inserter.Inserter) *TableManager {
	return &TableManager{
		table:       table.NewTable(*ts),
		rowsJsons:   []byte{},
		inserters:   inserters,
		maxRows:     int64(config.MaxRows),
		timeoutMs:   config.TimeoutMs,
		sendChannel: make(chan struct{}, 1),
		stopChannel: make(chan struct{}),
	}
}

//UpdateConfig updates maxRows and timeoutMs from config.
//Thread safe
func (tm *TableManager) UpdateConfig(config Config) {
	atomic.StoreInt64(&tm.maxRows, int64(config.MaxRows))
	atomic.StoreInt64(&tm.timeoutMs, config.TimeoutMs)
}

//AppendRowsToTable is a frontend for table's AppendRows.
//If maxRows is reached sends signal to start inserting (see Run)
func (tm *TableManager) AppendRowsToTable(rowsJSON []byte) error {
	tm.tableMut.Lock()
	err := tm.table.AppendRows(rowsJSON)
	tm.tableMut.Unlock()
	if tm.isTooManyRows() {
		log.Printf("reached max rows for table %s", tm.table.GetKey())
		select {
		case tm.sendChannel <- struct{}{}:
		default:
		}
	}

	return err
}

func (tm *TableManager) isTooManyRows() bool {
	tm.tableMut.Lock()
	rowsLen := tm.table.GetRowsLen()
	tm.tableMut.Unlock()

	return int64(rowsLen) >= atomic.LoadInt64(&tm.maxRows)
}

//Run is manager's main loop where is waiting for time limit
//or a signal to insert. The place where inserts should be fired
func (tm *TableManager) Run() {
	timer := tm.newTimer()
	for {
		stop := false
		select {
		case <-timer.C:
		case <-tm.sendChannel:
			if !tm.isTooManyRows() {
				continue
			}
			timer.Stop()
		case <-tm.stopChannel:
			stop = true
		}

		timer = tm.newTimer()
		err := tm.DoInsert()
		if err != nil {
			log.Println(err)
		}
		if stop {
			break
		}
	}
	tm.stopChannel <- struct{}{}
}

func (tm *TableManager) newTimer() *time.Timer {
	timeoutMs := atomic.LoadInt64(&tm.timeoutMs)
	return time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
}

//DoInsert creates a new table and calls inserters on the old
func (tm *TableManager) DoInsert() (err error) {
	if tm.isTableEmpty() {
		return nil
	}

	tbl := tm.getTableAndMakeNew()
	if len(tm.inserters) == 1 {
		for _, inserter := range tm.inserters {
			err = inserter.Insert(tbl)
		}
	} else {
		err = tm.insertConcurrently()
	}
	tbl.Free()

	return
}

func (tm *TableManager) isTableEmpty() bool {
	tm.tableMut.Lock()
	res := tm.table.GetRowsLen() == 0
	tm.tableMut.Unlock()

	return res
}

func (tm *TableManager) getTableAndMakeNew() *table.Table {
	ts := tm.table.Signature
	newTable := table.NewTable(ts)

	var oldTable *table.Table
	tm.tableMut.Lock()
	oldTable, tm.table = tm.table, newTable
	defer tm.tableMut.Unlock()

	return oldTable
}

func (tm *TableManager) insertConcurrently() error {
	errChan := make(chan error)
	defer close(errChan)
	for _, ins := range tm.inserters {
		go func(inserter inserter.Inserter, t table.Table) {
			errChan <- inserter.Insert(&t)
		}(ins, *tm.table)
	}
	errMessages := make([]string, 0, len(tm.inserters))
	for range tm.inserters {
		if err := <-errChan; err != nil {
			errMessages = append(errMessages, err.Error())
		}
	}
	if len(errMessages) != 0 {
		return errors.New(strings.Join(errMessages, ","))
	}

	return nil
}

//Stop sends a signal in main loop to insert,
//waits for response (which means the main loop is finished)
func (tm *TableManager) Stop() {
	key := tm.table.GetKey()
	log.Printf("stopping table manager for %s", key)
	tm.stopChannel <- struct{}{}
	<-tm.stopChannel
	log.Printf("stopped table manager for %s", key)
}
