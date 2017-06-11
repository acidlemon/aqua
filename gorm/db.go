package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"strconv"
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

	envval := os.Getenv("AQUA_DEBUG")
	val, err := strconv.Atoi(envval)
	if err == nil && val != 0 {
		d.LogMode(true)
	}

	envval = os.Getenv("AQUA_GORM_DISABLE_AUTO_TIMESTAMP")
	val, err = strconv.Atoi(envval)
	if err == nil && val != 0 {
		d.Callback().Create().Remove("gorm:update_time_stamp")
		d.Callback().Update().Remove("gorm:update_time_stamp")
	}

	return &db{root: d}, nil
}

func (db *db) GetProvider() interface{} {
	return db.root
}

func (_db *db) Begin(ctx context.Context, opts *sql.TxOptions) (aqua.Tx, error) {
	tx := _db.root.Begin()
	result := &db{
		root: tx,
	}

	return result, nil
}

func (db *db) Commit() error {
	db.root.Commit()
	errs := db.root.GetErrors()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
func (db *db) Rollback() error {
	db.root.Rollback()
	errs := db.root.GetErrors()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
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

func (db *db) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.root.CommonDB().Exec(query, args...)
}

func (db *db) Create(ctx context.Context, param ...interface{}) error {
	// TODO waiting support bulk insert
	for _, v := range param {
		db.session = db.session.Create(v)
	}
	return nil
}
func (db *db) Update(ctx context.Context, param interface{}) error {
	v := reflect.ValueOf(param)
	if v.Kind() == reflect.Map {
		db.session = db.session.Updates(param)
	} else {
		// TODO update using existing structパターンで
		// SET id=? WHERE id=?なクエリがでて気持ち悪いのをどうにかしたい
		db.session = db.session.Model(param).Update(param)
	}

	errs := db.session.GetErrors()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
func (db *db) Delete(ctx context.Context, param interface{}) error {
	db.session.Delete(param)
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

func (db *db) Where(condition string, bind ...interface{}) aqua.StmtCondition {
	if len(bind) == 1 {
		t := reflect.TypeOf(bind[0])
		//pp.Print(t)
		if t.Kind() == reflect.Slice {
			db.session = db.session.Where(condition, bind[0].([]interface{})...)
		} else {
			db.session = db.session.Where(condition, bind)
		}
	} else {
		db.session = db.session.Where(condition, bind...)
	}

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
	if len(values) == 1 {
		db.session = db.session.Where(fmt.Sprintf("%s in (?)", column), values...)
	} else {
		db.session = db.session.Where(fmt.Sprintf("%s in (?)", column), values)
	}
	return db
}

func (db *db) WhereBetween(column string, a, b interface{}) aqua.StmtCondition {
	db.session = db.session.Where(fmt.Sprintf("%s between ? and ?", column), a, b)
	return db
}

func (db *db) WhereLike(column, pattern string) aqua.StmtCondition {
	db.session = db.session.Where(fmt.Sprintf("%s like ?", column), pattern)
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
		session: db.session,
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
