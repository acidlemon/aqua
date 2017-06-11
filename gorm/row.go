package gorm

import (
	"database/sql"

	"github.com/jinzhu/gorm"
)

type row struct {
	session *gorm.DB
}

func (r *row) Scan(dest ...interface{}) error {
	rows, err := r.session.Model(dest).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		return rows.Scan(dest...)
	}

	return sql.ErrNoRows
}

func (r *row) ScanRow(dest interface{}) error {
	r.session.Model(dest).Scan(dest)

	return nil
}
