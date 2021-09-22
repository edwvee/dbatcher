package inserter

import (
	"log"

	"github.com/edwvee/dbatcher/internal/table"
)

type DummyInserter struct{}

func (ci *DummyInserter) Init(config Config) error {
	return nil
}

func (ci DummyInserter) Insert(t *table.Table) error {
	log.Printf(
		"Dummy: did nothing with %d rows",
		t.GetRowsLen(),
	)
	return nil
}
