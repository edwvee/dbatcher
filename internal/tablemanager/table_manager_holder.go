package tablemanager

import (
	"log"
	"sync"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/pkg/errors"
)

const maxTableManagerStopTime = 5 * time.Second

//not const for speedind up tests
var stopUnusedManagersInterval = 10 * time.Second

//ErrTableManagerDidntStopInTime means table manager didn't stop in time
var ErrTableManagerDidntStopInTime = errors.New("didn't stop in time")

//Holder creates table managers, holds pointers to them,
//stops them when they are not used for a long time.
//Serves as frontend to table managers.
type Holder struct {
	inserters         map[string]inserter.Inserter
	managers          map[string]*TableManager
	lastManagerVisit  map[string]time.Time
	managersMut       sync.Mutex
	insertErrorLogger *inserter.InsertErrorLogger
}

//NewHolder creates new holder
func NewHolder(errChan chan error, inserters map[string]inserter.Inserter, insertErrorLogger *inserter.InsertErrorLogger) *Holder {
	return &Holder{
		inserters:         inserters,
		managers:          map[string]*TableManager{},
		lastManagerVisit:  map[string]time.Time{},
		insertErrorLogger: insertErrorLogger,
	}
}

//Append searches for an existing table manager or creates it,
//then calls it's AppendRowsToTable. If sync is true, always creates a new manager
//and instantly calls DoInsert.
func (h *Holder) Append(ts *table.Signature, config Config, sync bool, rowsJSON []byte) error {
	if !sync {
		manager := h.getTableManager(ts, config)
		return manager.AppendRowsToTable(rowsJSON)
	}

	//not optimized due sync is debug feature
	manager := NewTableManager(ts, config, h.inserters, h.insertErrorLogger)
	if err := manager.AppendRowsToTable(rowsJSON); err != nil {
		return err
	}
	return manager.DoInsert()
}

func (h *Holder) getTableManager(ts *table.Signature, config Config) *TableManager {
	key := ts.GetKey()

	h.managersMut.Lock()
	manager, ok := h.managers[key]
	if !ok {
		log.Printf("new table: %s", key)
		manager = NewTableManager(ts, config, h.inserters, h.insertErrorLogger)
		go manager.Run()
		h.managers[key] = manager
	}
	h.lastManagerVisit[key] = time.Now()
	h.managersMut.Unlock()

	if ok {
		manager.UpdateConfig(config)
	}

	return manager
}

//StopUnusedManagers starts a goroutine which stops unused
//table managers periodically.
func (h *Holder) StopUnusedManagers() {
	go h.stopUnusedManagers()
}

func (h *Holder) stopUnusedManagers() {
	ticker := time.NewTicker(stopUnusedManagersInterval)
	for range ticker.C {
		unusedManagers := []*TableManager{}
		now := time.Now()
		h.managersMut.Lock()
		for key, lastVisited := range h.lastManagerVisit {
			if now.Sub(lastVisited) > stopUnusedManagersInterval {
				unusedManagers = append(unusedManagers, h.managers[key])
				delete(h.managers, key)
				delete(h.lastManagerVisit, key)
			}
		}
		h.managersMut.Unlock()

		for _, manager := range unusedManagers {
			go manager.Stop()
		}
	}
}

//StopTableManagers stops existing table managers with timeout.
//If one of them didn't stop in time or has insert errors returns them.
func (h *Holder) StopTableManagers() []error {
	h.managersMut.Lock()
	errs := []error{}
	errChan := make(chan error)
	for name, manager := range h.managers {
		go func(name string, manager *TableManager) {
			timer := time.NewTimer(maxTableManagerStopTime)
			stopChan := make(chan struct{})
			go func(manager *TableManager, stopChan chan struct{}) {
				manager.Stop()
				stopChan <- struct{}{}
			}(manager, stopChan)

			var err error
			select {
			case <-timer.C:
				err = errors.Wrapf(ErrTableManagerDidntStopInTime, "table manager %s", name)
			case <-stopChan:
			}
			errChan <- err
		}(name, manager)
	}
	for range h.managers {
		err := <-errChan
		if err != nil {
			errs = append(errs, err)
		}
	}
	h.managersMut.Unlock()

	return errs
}
