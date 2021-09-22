package table

import "sync"

const (
	startTableDataSize = 10000
	maxTableDataSize   = startTableDataSize * 10
)

var tableDataPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, startTableDataSize)
	},
}
