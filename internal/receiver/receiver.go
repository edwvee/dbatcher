package receiver

import (
	"github.com/edwvee/dbatcher/internal/tablemanager"
)

//Receiver is something that communicates with the outer world and receives data
type Receiver interface {
	Init(config Config, errChan chan error, tableManagerHolder *tablemanager.Holder) error
	Receive()
	Stop() error
}
