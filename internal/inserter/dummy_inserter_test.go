package inserter

import (
	"testing"

	"github.com/edwvee/dbatcher/internal/table"
)

//It's kinda stupid to test DummyInserter.
//Well, if test not fails then DummyInserter won't cause error.

func TestDummyInit(t *testing.T) {
	ins := DummyInserter{}
	if err := ins.Init(Config{}); err != nil {
		t.Error(err)
	}
}

func TestDummyInsert(t *testing.T) {
	ins := DummyInserter{}
	if err := ins.Init(Config{}); err != nil {
		t.Fatal(err)
	}
	ts := table.NewTableSignature("table", "field1")
	table := table.NewTable(ts)
	if err := table.AppendRows([]byte("[[1],[2],[3]]")); err != nil {
		t.Fatal(err)
	}
	if err := ins.Insert(table); err != nil {
		t.Error(err)
	}
}
