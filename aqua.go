package aqua

// import "gopkg.in/acidlemon/aqua.v0"

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type DB interface {
	Begin(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Close() error
	Driver() driver.Driver
	Ping(ctx context.Context) error
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)

	QueryRunner

	// for customize original provider
	GetProvider() interface{}
}

type Tx interface {
	Commit() error
	Rollback() error

	QueryRunner
}

type QueryRunner interface {
	Table(name string) StmtTable
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type StmtTable interface {
	StmtCondition
	Join(table, condition string) StmtTable
	LeftJoin(table, condition string) StmtTable
	RightJoin(table, condition string) StmtTable
	Select(columns ...string) StmtTable

	Create(ctx context.Context, values ...interface{}) error
}

type StmtCondition interface {
	StmtAggregate

	// Where系は検討の余地がたくさんあって悩んでいる
	Where(condition string, bind ...interface{}) StmtCondition
	WhereEq(column string, value interface{}) StmtCondition
	WhereIn(column string, values ...interface{}) StmtCondition
	WhereBetween(column string, a, b interface{}) StmtCondition
	WhereLike(column, pattern string) StmtCondition
}

type StmtAggregate interface {
	StmtRunner

	// こいつらは2回呼ぶと上書き、もしくはpanicさせたほうがいいか
	GroupBy(columns ...string) StmtAggregate
	OrderBy(columns ...string) StmtAggregate
	Having(condition string) StmtAggregate
	LimitOffset(limit, offset int) StmtAggregate
}

type StmtRunner interface {
	All(ctx context.Context) (Rows, error)
	Single(ctx context.Context) (Row, error)
	FetchColumn(ctx context.Context, column string) (Rows, error)
	Count(ctx context.Context) (int, error)

	Update(ctx context.Context, v interface{}) error
	Delete(ctx context.Context, v interface{}) error
}

type Row interface {
	ScanRow(dest interface{}) error
	Scan(dest ...interface{}) error
}

type Rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool

	Scan(dest ...interface{}) error // sql.Rows 's Scan()
	ScanAll(dest interface{}) error
}
