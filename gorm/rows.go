package gorm

import (
	"database/sql"
	"fmt"
	"reflect"
)

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
