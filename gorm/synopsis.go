package gorm

import (
	"context"
	"database/sql"

	"github.com/acidlemon/aqua"
)

// For compile check... (TODO 後でTestにする)
func Hoge() {

	ctx := context.Background()

	cfg := aqua.Config{
		Host:     "127.0.0.1",
		Port:     "3306",
		UserName: "root",
		Password: "",
		Database: "microsvc",
	}

	db, err := Open("mysql", cfg.DSN("mysql"))
	if err != nil {
		panic("cannot open db: " + err.Error())
	}

	rows, err := db.Table("test").WhereEq("id", 42).All(ctx)
	if err != nil && err != sql.ErrNoRows {
		panic(`something error on select * from test where id = 42 ` + err.Error())
	}
	type Test struct {
		ID int
	}
	ts := []Test{}
	rows.Scan(&ts)

	rows, err = db.Table("test").WhereEq("tag", "powawa").FetchColumn(ctx, "id")
	if err != nil && err != sql.ErrNoRows {
		panic(`something error on select id from test where tag = 'powawa' ` + err.Error())
	}
	ids := []int{}
	rows.Scan(&ids)

}
