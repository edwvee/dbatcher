package inserter

import (
	"database/sql"
	"log"
	"strings"
	"time"

	"github.com/edwvee/dbatcher/internal/table"
	_ "github.com/go-sql-driver/mysql" //golint: MysqlInserter won't really work without it
)

//MysqlInserter inserts rows into MySQL
type MysqlInserter struct {
	db            *sql.DB
	insertTimeout time.Duration
}

//Init setups MysqlInserter and connects to mysql
func (mi *MysqlInserter) Init(config Config) error {
	db, err := connectDB("mysql", config.Dsn, config.MaxConnections)
	if err != nil {
		return err
	}
	mi.db = db
	mi.insertTimeout = time.Duration(config.InsertTimeoutMs) * time.Millisecond

	return nil
}

//Insert inserts rows to mysql
func (mi MysqlInserter) Insert(t *table.Table) error {
	sqlStr := mi.makeSQL(t)
	println(sqlStr)
	start := time.Now()
	count, err := mi.insert(t, sqlStr)
	if err != nil {
		return err
	}
	passed := time.Since(start)
	log.Printf(
		"MySQL: inserted %d rows for %s; Query: %s",
		count, passed.String(), strings.Split(sqlStr, "VALUES")[0],
	)
	return nil
}

func (mi MysqlInserter) insert(t *table.Table, sqlStr string) (count int64, err error) {
	res, err := mi.db.Exec(sqlStr, t.GetRawData()...)
	if err == nil {
		count, _ = res.RowsAffected()
	}
	return count, err
}

func (mi MysqlInserter) makeSQL(t *table.Table) string {
	qsPerRow := strings.Count(t.GetFields(), ",") + 1
	rowQsSlice := make([]string, qsPerRow)
	for i := range rowQsSlice {
		rowQsSlice[i] = "?"
	}
	rowQs := "(" + strings.Join(rowQsSlice, ",") + ")"
	allQsSice := make([]string, t.GetRowsLen())
	for i := range allQsSice {
		allQsSice[i] = rowQs
	}

	return "INSERT IGNORE INTO " + t.GetTableName() +
		"(" + t.GetFields() + ") VALUES " + strings.Join(allQsSice, ",")
}
