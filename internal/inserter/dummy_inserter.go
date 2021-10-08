package inserter

import (
	"log"

	"github.com/edwvee/dbatcher/internal/table"
)

//DummyInserter doesn't write data, only reports
type DummyInserter struct{}

//Init does nothing
func (ci *DummyInserter) Init(config Config) error {
	return nil
}

//Insert reports about table's rows count
func (ci DummyInserter) Insert(t *table.Table) error {
	log.Printf(
		"Dummy: did nothing with %d rows",
		t.GetRowsLen(),
	)
	return nil
}
