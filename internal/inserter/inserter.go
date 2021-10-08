package inserter

import "github.com/edwvee/dbatcher/internal/table"

//Inserter inserts table's rows to a specific DMBS or other destination
type Inserter interface {
	Init(config Config) error
	Insert(t *table.Table) error
}
