package aqua

import (
	"database/sql"
	"database/sql/driver"
)

type db struct {
	origin *sql.DB
}

func (d *db) Begin() (Tx, error) {
	t, err := d.origin.Begin()
	if err != nil {
		return nil, err
	}

	return &tx{origin: t}, nil
}

func (d *db) Close() error {
	return d.origin.Close()
}
func (d *db) Driver() driver.Driver {
	return d.origin.Driver()
}
func (d *db) Ping() error {
	return d.origin.Ping()
}
func (d *db) SetMaxIdleConns(n int) {
	d.origin.SetMaxIdleConns(n)
}
func (d *db) SetMaxOpenConns(n int) {
	d.origin.SetMaxOpenConns(n)
}
func (d *db) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.origin.Exec(query, args...)
}
func (d *db) Prepare(query string) (*sql.Stmt, error) {
	return d.origin.Prepare(query)
}
func (d *db) Query(query string, args ...interface{}) (Rows, error) {
	return d.origin.Query(query, args...)
}
func (d *db) QueryRow(query string, args ...interface{}) Row {
	return d.origin.QueryRow(query, args...)
}

type tx struct {
	origin *sql.Tx
}

func (t *tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.origin.Exec(query, args...)
}
func (t *tx) Prepare(query string) (*sql.Stmt, error) {
	return t.origin.Prepare(query)
}
func (t *tx) Query(query string, args ...interface{}) (Rows, error) {
	return t.origin.Query(query, args...)
}
func (t *tx) QueryRow(query string, args ...interface{}) Row {
	return t.origin.QueryRow(query, args...)
}
func (t *tx) Commit() error {
	return t.origin.Commit()
}
func (t *tx) Rollback() error {
	return t.origin.Rollback()
}
func (t *tx) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return t.origin.Stmt(stmt)
}
