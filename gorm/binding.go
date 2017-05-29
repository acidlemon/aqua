package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/acidlemon/aqua"
	"github.com/jinzhu/gorm"
)

type db struct {
	d *gorm.DB

	pluckColumn string
}

func init() {
	aqua.RegisterProvider("gorm", Open)
}

func Open(driver, path string) (aqua.DB, error) {
	d, err := gorm.Open(driver, path)
	if err != nil {
		return nil, err
	}

	d.LogMode(true)

	return &db{d: d}, nil
}

func (db *db) Begin(ctx context.Context, opts *sql.TxOptions) (aqua.Tx, error) {
	return aqua.Tx(nil), nil
}

func (db *db) Close() error {
	return db.d.Close()
}

func (db *db) Driver() driver.Driver {
	return db.d.DB().Driver()
}

func (db *db) Ping(ctx context.Context) error {
	return db.d.DB().PingContext(ctx)
}

func (db *db) SetMaxIdleConns(conn int) {
	db.d.DB().SetMaxIdleConns(conn)
}

func (db *db) SetMaxOpenConns(conn int) {
	db.d.DB().SetMaxOpenConns(conn)
}

func (db *db) Table(name string) aqua.StmtTable {
	db.d = db.d.Table(name)
	return db
}

func (db *db) Create(ctx context.Context, param ...interface{}) error {
	return nil
}
func (db *db) Update(ctx context.Context, param interface{}) error {
	return nil
}
func (db *db) Delete(ctx context.Context, param interface{}) error {
	return nil
}

func (db *db) Join(join string) aqua.StmtTable {
	db.d = db.d.Joins(join)
	return db
}

func (db *db) Select(columns ...string) aqua.StmtTable {
	return db
}

func (db *db) Where(condition string) aqua.StmtCondition {
	return db
}

func (db *db) WhereEq(column string, value interface{}) aqua.StmtCondition {
	return db
}

func (db *db) WhereIn(column string, values ...interface{}) aqua.StmtCondition {
	db.d = db.d.Where(fmt.Sprintf("%s in (?)", values))
	return db
}

func (db *db) WhereBetween(column string, a, b interface{}) aqua.StmtCondition {
	return db
}

func (db *db) All(ctx context.Context) (aqua.Rows, error) {
	return nil, nil
}

func (db *db) Count(ctx context.Context) (int, error) {
	var cnt int
	db.d.Count(&cnt)
	if errs := db.d.GetErrors(); len(errs) != 0 {
		return 0, errs[len(errs)-1]
	}

	return cnt, nil
}

func (db *db) FetchColumn(ctx context.Context, column string) (aqua.Rows, error) {
	db.d = db.d.Select(column)

	rs := &rows{
		d:     db,
		pluck: true,
	}
	return rs, nil
}

func (db *db) Single(ctx context.Context) (aqua.Row, error) {
	return nil, nil
}

func (db *db) GroupBy(...string) aqua.StmtAggregate {
	return db
}

func (db *db) OrderBy(...string) aqua.StmtAggregate {
	return db
}

func (db *db) Having(string) aqua.StmtAggregate {
	return db
}

func (db *db) LimitOffset(limit, offset int) aqua.StmtAggregate {
	return db
}

type rows struct {
	d       *db
	pluck   bool
	sqlRows *sql.Rows
}

func (r *rows) Scan(dest interface{}) error {
	if !r.pluck {
		r.d.d.Model(dest).Scan(dest)
		return nil
	}

	sqlRows, err := r.d.d.Rows()
	if err != nil {
		return err
	}
	defer sqlRows.Close()

	container := reflect.Indirect(reflect.ValueOf(dest))
	if container.Kind() != reflect.Slice {
		return fmt.Errorf(`dest should be a slice, not %s`, container.Kind())
	}

	for sqlRows.Next() {
		elem := reflect.New(container.Type().Elem()).Interface()
		sqlRows.Scan(elem)
		container.Set(reflect.Append(container, reflect.ValueOf(elem).Elem()))
	}

	return nil
}

func (r *rows) Close() error {
	if r.sqlRows == nil {
		sqlRows, err := r.d.d.Rows()
		if err != nil {
			return err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Close()
}

func (r *rows) Columns() ([]string, error) {
	if r.sqlRows == nil {
		sqlRows, err := r.d.d.Rows()
		if err != nil {
			return nil, err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Columns()
}
func (r *rows) Err() error {
	if r.sqlRows == nil {
		sqlRows, err := r.d.d.Rows()
		if err != nil {
			return err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Err()
}
func (r *rows) Next() bool {
	if r.sqlRows == nil {
		sqlRows, err := r.d.d.Rows()
		if err != nil {
			return false
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Next()
}
