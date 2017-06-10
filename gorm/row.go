package gorm

type row struct {
	db *db
}

func (r *row) Scan(dest interface{}) error {
	r.db.session.Model(dest).Scan(dest)

	return nil
}
