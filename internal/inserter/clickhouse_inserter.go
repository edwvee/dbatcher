package inserter

import (
	"context"
	"database/sql"
	"log"
	"net/url"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/edwvee/dbatcher/internal/table"
	"github.com/pkg/errors"
)

var (
	ErrNoDatabaseInDsnOrInTableName = errors.New("no database in dsn or in table name")
	ErrNoSuchTableStructure         = errors.New("no column info for a table")
)

type ClickHouseInserter struct {
	db            *sql.DB
	databaseName  string
	insertTimeout time.Duration
}

func (ci *ClickHouseInserter) Init(config Config) error {
	db, err := connectDB(config.Type, config.Dsn, config.MaxConnections)
	if err != nil {
		return err
	}
	ci.db = db
	ci.insertTimeout = time.Duration(config.InsertTimeoutMs) * time.Millisecond
	u, err := url.Parse(config.Dsn)
	if err != nil {
		return err
	}
	ci.databaseName = u.Query().Get("database")

	return nil
}

func (ci ClickHouseInserter) Insert(t *table.Table) error {
	sqlStr := ci.makeSql(t)
	start := time.Now()
	if err := ci.insert(t, sqlStr); err != nil {
		return err
	}
	passed := time.Since(start)
	log.Printf(
		"ClickHouse: inserted %d rows for %s; Query: %s",
		t.GetRowsLen(), passed.String(), sqlStr,
	)
	return nil
}

func (ci ClickHouseInserter) insert(t *table.Table, sqlStr string) error {
	structure, err := ci.getTableStructure(t)
	if err != nil {
		return err
	}
	fields := strings.Split(t.GetFields(), ",")
	for i, field := range fields {
		fields[i] = strings.TrimSpace(field)
	}
	//TODO: wrap errors
	ctx, cancel := context.WithTimeout(context.Background(), ci.insertTimeout)
	defer cancel()

	tx, err := ci.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for row := t.GetNextRow(); row != nil; row = t.GetNextRow() {
		converted, err := structure.ConvertJsonRow(fields, row)
		if err != nil {
			tx.Rollback()
			return err
		}
		//TODO: !!!!! error must work
		_, err = stmt.Exec(converted...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	//TODO: wrap too
	return tx.Commit()
}

func (ci ClickHouseInserter) makeSql(t *table.Table) string {
	//TODO: question marks
	return "INSERT INTO " + t.GetTableName() +
		"(" + t.GetFields() + ") VALUES (?,?,?,?)"
}

func (ci ClickHouseInserter) getTableStructure(t *table.Table) (structure clickhouseStructure, err error) {
	var database, table string
	tName := t.GetTableName()
	if pos := strings.Index(tName, "."); pos != -1 {
		database = tName[0:pos]
		table = tName[pos+1:]
	} else {
		if ci.databaseName == "" {
			return structure, ErrNoDatabaseInDsnOrInTableName
		}
		database = ci.databaseName
		table = tName
	}
	database = strings.Replace(database, "`", "", -1)
	table = strings.Replace(table, "`", "", -1)

	var column string
	var chType clickhouseType
	sqlStr := "SELECT name, type FROM system.columns WHERE database = ? AND `table` = ?"
	rows, err := ci.db.Query(sqlStr, database, table)
	if err != nil {
		return structure, errors.Wrapf(err, "get table structure for %s:", t.GetKey())
	}
	defer rows.Close()

	structure = clickhouseStructure{}
	for rows.Next() {
		err = rows.Scan(&column, &chType)
		if err != nil {
			return
		}
		if strings.HasPrefix(chType, "Enum") || strings.HasPrefix(chType, "FixedString") || strings.HasPrefix(chType, "DateTime64") {
			chType = chType[0:strings.Index(chType, "(")]
		}
		structure[column] = chType
	}
	if len(structure) == 0 {
		err = ErrNoSuchTableStructure
	}

	return structure, errors.Wrapf(err, "get table structure for %s:", t.GetKey())
}
