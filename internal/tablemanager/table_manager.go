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

func NewTableManager(ts *table.TableSignature, config TableManagerConfig, inserters map[string]inserter.Inserter) *TableManager {
	return &TableManager{
		table:       table.NewTable(*ts),
		rowsJsons:   []byte{},
		inserters:   inserters,
		maxRows:     int64(config.MaxRows),
		timeoutMs:   config.TimeoutMs,
		sendChannel: make(chan struct{}, 1),
		stopChannel: make(chan struct{}),

		//TODO: a lot
	}
}

func (tm *TableManager) UpdateConfig(config TableManagerConfig) {
	atomic.StoreInt64(&tm.maxRows, int64(config.MaxRows))
	atomic.StoreInt64(&tm.timeoutMs, config.TimeoutMs)
}

func (tm *TableManager) AppendRowsToTable(rowsJson []byte) error {
	tm.tableMut.Lock()
	err := tm.table.AppendRows(rowsJson)
	tm.tableMut.Unlock()
	if tm.IsTooManyRows() {
		log.Printf("reached max rows for table %s", tm.table.GetKey())
		select {
		case tm.sendChannel <- struct{}{}:
		default:
		}
	}

	return err
}

func (tm *TableManager) IsTooManyRows() bool {
	tm.tableMut.Lock()
	rowsLen := tm.table.GetRowsLen()
	tm.tableMut.Unlock()

	return int64(rowsLen) >= atomic.LoadInt64(&tm.maxRows)
}

func (tm *TableManager) Run() {
	timer := tm.newTimer()
	for {
		stop := false
		select {
		case <-timer.C:
		case <-tm.sendChannel:
			if !tm.IsTooManyRows() {
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

//TODO: remove do lock cause it'll freeze request (or not)
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
		err = tm.insertConcurrently(tbl)
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
	ts := tm.table.TableSignature
	newTable := table.NewTable(ts)

	var oldTable *table.Table
	tm.tableMut.Lock()
	oldTable, tm.table = tm.table, newTable
	defer tm.tableMut.Unlock()

	return oldTable
}

func (tm *TableManager) insertConcurrently(tbl *table.Table) error {
	errChan := make(chan error)
	defer close(errChan)
	for _, ins := range tm.inserters {
		go func(inserter inserter.Inserter, t table.Table) {
			errChan <- inserter.Insert(&t)
		}(ins, *tbl)
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

func (tm *TableManager) Stop() {
	key := tm.table.GetKey()
	log.Printf("stopping table manager for %s", key)
	tm.stopChannel <- struct{}{}
	<-tm.stopChannel
	log.Printf("stopped table manager for %s", key)
}
