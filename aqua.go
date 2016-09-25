package aqua

// import "gopkg.in/acidlemon/aqua.v0"

import (
	"database/sql"
	"database/sql/driver"
)

//var db *sql.DB

func Setup(string) {

}

func Open(driver, path string) (DB, error) {
	d, err := sql.Open(driver, path)
	if err != nil {
		return nil, err
	}

	return &db{origin: d}, nil
}

type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
}

type DB interface {
	// sql.DB
	Begin() (Tx, error)
	Close() error
	Driver() driver.Driver
	Ping() error
	Executor
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
}

type Tx interface {
	// sql.Tx
	Executor
	Commit() error
	Rollback() error
	Stmt(stmt *sql.Stmt) *sql.Stmt
}

type Row interface {
	// sql.Row
	Scan(dest ...interface{}) error

	// aqua expantion
	//	ScanObject(obj ...interface{}) error
}

type Rows interface {
	// sql.Rows
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...interface{}) error

	// aqua expantion
	//	ScanObject(obj ...interface{}) error
}
