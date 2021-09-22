package inserter

import (
	"database/sql"
	"time"
)

func connectDB(dbType string, dsn string, dbMaxConnections int) (db *sql.DB, err error) {
	db, err = sql.Open(dbType, dsn)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}
	db.SetMaxOpenConns(dbMaxConnections)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(60 * time.Second)

	return
}
