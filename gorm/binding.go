package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"github.com/acidlemon/aqua"
	"github.com/jinzhu/gorm"
)

type db struct {
	root    *gorm.DB
	session *gorm.DB
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

	return &db{root: d}, nil
}

func (db *db) Begin(ctx context.Context, opts *sql.TxOptions) (aqua.Tx, error) {
	return aqua.Tx(nil), nil
}

func (db *db) Close() error {
	return db.root.Close()
}

func (db *db) Driver() driver.Driver {
	return db.root.DB().Driver()
}

func (db *db) Ping(ctx context.Context) error {
	return db.root.DB().PingContext(ctx)
}

func (db *db) SetMaxIdleConns(conn int) {
	db.root.DB().SetMaxIdleConns(conn)
}

func (db *db) SetMaxOpenConns(conn int) {
	db.root.DB().SetMaxOpenConns(conn)
}

func (db *db) Table(name string) aqua.StmtTable {
	db.session = db.root.Table(name)
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

func (db *db) Join(table, condition string) aqua.StmtTable {
	db.session = db.session.Joins(fmt.Sprintf("INNER JOIN %s ON %s", table, condition))
	return db
}

func (db *db) LeftJoin(table, condition string) aqua.StmtTable {
	db.session = db.session.Joins(fmt.Sprintf("LEFT JOIN %s ON %s", table, condition))
	return db
}

func (db *db) RightJoin(table, condition string) aqua.StmtTable {
	db.session = db.session.Joins(fmt.Sprintf("RIGHT JOIN %s ON %s", table, condition))
	return db
}

func (db *db) Select(columns ...string) aqua.StmtTable {
	db.session = db.session.Select(strings.Join(columns, ", "))
	return db
}

func (db *db) Where(condition string) aqua.StmtCondition {
	return db
}

func (db *db) WhereEq(column string, value interface{}) aqua.StmtCondition {
	if value == nil {
		db.session = db.session.Where(fmt.Sprintf("%s IS NULL", column))
	} else {
		db.session = db.session.Where(fmt.Sprintf("%s = ?", column), value)
	}
	return db
}

func (db *db) WhereIn(column string, values ...interface{}) aqua.StmtCondition {
	db.session = db.session.Where(fmt.Sprintf("%s in (?)", column), values...)
	return db
}

func (db *db) WhereBetween(column string, a, b interface{}) aqua.StmtCondition {
	return db
}

func (db *db) All(ctx context.Context) (aqua.Rows, error) {
	rs := &rows{
		db: db,
	}
	return rs, nil
}

func (db *db) Count(ctx context.Context) (int, error) {
	var cnt int
	db.session.Count(&cnt)
	if errs := db.session.GetErrors(); len(errs) != 0 {
		return 0, errs[len(errs)-1]
	}

	return cnt, nil
}

func (db *db) FetchColumn(ctx context.Context, column string) (aqua.Rows, error) {
	db.session = db.session.Select(column)

	rs := &rows{
		db:    db,
		pluck: true,
	}
	return rs, nil
}

func (db *db) Single(ctx context.Context) (aqua.Row, error) {
	db.session = db.session.Limit(1)

	r := &row{
		db: db,
	}
	return r, nil
}

func (db *db) GroupBy(groups ...string) aqua.StmtAggregate {
	db.session = db.session.Group(strings.Join(groups, ","))
	return db
}

func (db *db) OrderBy(orders ...string) aqua.StmtAggregate {
	db.session = db.session.Order(strings.Join(orders, ","))
	return db
}

func (db *db) Having(string) aqua.StmtAggregate {
	return db
}

func (db *db) LimitOffset(limit, offset int) aqua.StmtAggregate {
	return db
}

type rows struct {
	db      *db
	pluck   bool
	sqlRows *sql.Rows
}

func (r *rows) Scan(dest interface{}) error {
	if !r.pluck {
		r.db.session.Model(dest).Scan(dest)
		return nil
	}

	// copy from gorm scan
	sqlRows, err := r.db.session.Rows()
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
		sqlRows, err := r.db.session.Rows()
		if err != nil {
			return err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Close()
}

func (r *rows) Columns() ([]string, error) {
	if r.sqlRows == nil {
		sqlRows, err := r.db.session.Rows()
		if err != nil {
			return nil, err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Columns()
}
func (r *rows) Err() error {
	if r.sqlRows == nil {
		sqlRows, err := r.db.session.Rows()
		if err != nil {
			return err
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Err()
}
func (r *rows) Next() bool {
	if r.sqlRows == nil {
		sqlRows, err := r.db.session.Rows()
		if err != nil {
			return false
		}

		r.sqlRows = sqlRows
	}

	return r.sqlRows.Next()
}

type row struct {
	db *db
}

func (r *row) Scan(dest interface{}) error {
	r.db.session.Model(dest).Scan(dest)

	return nil
}
