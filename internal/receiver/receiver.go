package receiver

import (
	"github.com/edwvee/dbatcher/internal/tablemanager"
)

type Receiver interface {
	Init(config Config, errChan chan error, tableManagerHolder *tablemanager.TableManagerHolder) error
	Receive()
	Stop() error
}
