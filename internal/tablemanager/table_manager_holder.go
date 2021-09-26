package tablemanager

import (
	"log"
	"sync"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/pkg/errors"
)

const maxTableMangerStopTime = 5 * time.Second
const stopUnusedManagersInterval = 10 * time.Second

var ErrTableManagerDidntStopInTime = errors.New("didn't stop in time")

type TableManagerHolder struct {
	inserters        map[string]inserter.Inserter
	managers         map[string]*TableManager
	lastManagerVisit map[string]time.Time
	managersMut      sync.Mutex
}

func NewTableManagerHolder(errChan chan error, inserters map[string]inserter.Inserter) *TableManagerHolder {
	return &TableManagerHolder{
		inserters:        inserters,
		managers:         map[string]*TableManager{},
		lastManagerVisit: map[string]time.Time{},
	}
}

//TODO: validation
func (h *TableManagerHolder) Append(ts *table.TableSignature, config TableManagerConfig, sync bool, rowsJson []byte) error {
	if !sync {
		manager := h.getTableManager(ts, config)
		return manager.AppendRowsToTable(rowsJson)
	}

	//not optimized due sync is debug feature
	manager := NewTableManager(ts, config, h.inserters)
	if err := manager.AppendRowsToTable(rowsJson); err != nil {
		return err
	}
	return manager.DoInsert()
}

func (h *TableManagerHolder) getTableManager(ts *table.TableSignature, config TableManagerConfig) *TableManager {
	key := ts.GetKey()

	h.managersMut.Lock()
	manager, ok := h.managers[key]
	if !ok {
		log.Printf("new table: %s", key)
		manager = NewTableManager(ts, config, h.inserters)
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

func (h *TableManagerHolder) StopUnusedManagers() {
	go h.stopUnusedManagers()
}

func (h *TableManagerHolder) stopUnusedManagers() {
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

func (h *TableManagerHolder) StopTableManagers() []error {
	h.managersMut.Lock()
	errs := []error{}
	errChan := make(chan error)
	for name, manager := range h.managers {
		go func(name string, manager *TableManager) {
			timer := time.NewTimer(maxTableMangerStopTime)
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
