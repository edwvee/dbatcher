package tablemanager

import (
	"sync"
	"time"

	"github.com/edwvee/dbatcher/internal/inserter"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/pkg/errors"
)

const maxTableMangerStopTime = 5 * time.Second

var ErrTableManagerDidntStopInTime = errors.New("didn't stop in time")

type TableManagerHolder struct {
	inserters   map[string]inserter.Inserter
	managers    map[string]*TableManager
	managersMut sync.Mutex
}

func NewTableManagerHolder(errChan chan error, inserters map[string]inserter.Inserter) *TableManagerHolder {
	return &TableManagerHolder{
		inserters: inserters,
		managers:  map[string]*TableManager{},
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
		manager = NewTableManager(ts, config, h.inserters)
		go manager.Run()
		h.managers[key] = manager
	}
	h.managersMut.Unlock()

	if ok {
		manager.UpdateConfig(config)
	}

	return manager
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
