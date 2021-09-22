package inserter

import "github.com/edwvee/dbatcher/internal/table"

type Inserter interface {
	Init(config Config) error
	Insert(t *table.Table) error
}
