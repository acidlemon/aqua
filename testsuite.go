package aqua

import (
	"context"
	"testing"
	"time"
)

// test suite for provider

func NewTestSuite(t *testing.T, provider string) *TestSuite {
	return &TestSuite{
		T:        t,
		provider: provider,
	}
}

type TestSuite struct {
	*testing.T
	provider string
	db       DB
}

func (t *TestSuite) Run() {
	t.testDB()
	t.testCreate()
	t.testJoin()
	t.testWhere()
	t.testAggregation()
	t.testMisc()
}

func (t *TestSuite) testDB() {
	ctx := context.Background()

	db, err := Open(t.provider, "sqlite3", ":memory:")
	if err != nil {
		t.Fatalf(`cannot open database: %s`, err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE test (
id INTEGER PRIMARY KEY AUTOINCREMENT,
data VARCHAR(80),
person_id INTEGER NULL
)`)
	if err != nil {
		t.Fatalf(`failed to crate table: %s`, err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE person (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name VARCHAR(80),
created_at TIMESTAMP
)`)
	if err != nil {
		t.Fatalf(`failed to crate table: %s`, err)
	}

	t.db = db
}

func (t *TestSuite) testMisc() {
	// just call, no check
	t.db.GetProvider()

	ctx := context.Background()

	err := t.db.Ping(ctx)
	if err != nil {
		t.Errorf(`something happen on Ping: %s`, err)
	}

	// just call, no check
	t.db.SetMaxIdleConns(10)
	t.db.SetMaxOpenConns(10)

	// see you again
	err = t.db.Close()
	if err != nil {
		t.Errorf(`something happen on Close: %s`, err)
	}
}

type testRow struct {
	ID       int
	Data     string
	PersonID int
}
type personRow struct {
	ID        int
	Name      string
	CreatedAt time.Time
}

func (t *TestSuite) testCreate() {
	ctx := context.Background()

	// scenario1: simple insert
	{
		r := testRow{
			Data: "The quick brown fox jumps over the lazy dog",
		}

		err := t.db.Table("test").Create(ctx, &r)
		if err != nil {
			t.Fatalf(`failed to create row: %s`, err)
		}

		cnt, err := t.db.Table("test").Count(ctx)
		if err != nil {
			t.Fatalf(`failed to count of test table: %s`, err)
		}

		if cnt != 1 {
			t.Errorf(`expect count is 1, but actual %d`, cnt)
		}
		if r.ID != 1 {
			t.Errorf(`expect autofilled id is 1, but actual %d`, r.ID)
		}

		row, err := t.db.Table("test").Single(ctx)
		if err != nil {
			t.Fatalf(`failed to find test.id = 1 row: %s`, err)
		}

		var r2 testRow

		err = row.Scan(&r2)
		if err != nil {
			t.Fatalf(`failed to scan row: %s`, err)
		}

		if r2.ID != 1 {
			t.Errorf(`expected id is 1 but expected id is %d`, r2.ID)
		}

	}

	// scenario2: bulk insert
	{
		rs := []interface{}{
			&testRow{
				Data: "アフターファイブでイケイケ",
			},
			&testRow{
				Data: "666 666 6666666",
			},
			&testRow{
				Data: "Boeing 777-300ER",
			},
		}

		err := t.db.Table("test").Create(ctx, rs...)
		if err != nil {
			t.Fatalf(`failed to create row: %s`, err)
		}

		cnt, err := t.db.Table("test").Count(ctx)
		if err != nil {
			t.Fatalf(`failed to count of test table: %s`, err)
		}

		if cnt != 4 {
			t.Errorf(`expect count is 4, but actual %d`, cnt)
		}

		rows, err := t.db.Table("test").Where("id > 1").OrderBy("id DESC").All(ctx)
		if err != nil {
			t.Fatalf(`failed to fetch test.id > 1 rows: %s`, err)
		}

		r2 := []*testRow{}

		err = rows.Scan(&r2)
		if err != nil {
			t.Fatalf(`failed to scan row: %s`, err)
		}

		if len(r2) != 3 {
			t.Errorf(`expected row count is 3 but actual count %d`, len(r2))
		}

		for i, v := range r2 {
			if v.ID != 4-i {
				t.Errorf(`expected row id is %d, but actual id is %d`, 4-i, v.ID)
			}
			row := rs[2-i].(*testRow)
			if v.Data != row.Data {
				t.Errorf(`expected row data is %s, but actual data is %s`,
					row.Data, v.Data)
			}
		}
	}
}

func (t *TestSuite) mustTime(timeString string) time.Time {
	const timeFormat string = "2006-01-02 15:04:05"
	tm, err := time.ParseInLocation(timeFormat, timeString, time.Local)
	if err != nil {
		t.Fatalf(`time parsing error: %s`, err)
	}

	return tm
}

func (t *TestSuite) testJoin() {
	ctx := context.Background()

	// prepare data
	err := t.db.Table("person").Create(ctx, []interface{}{
		&personRow{
			Name:      "acidlemon",
			CreatedAt: t.mustTime("2017-06-10 16:40:50"),
		},
		&personRow{
			Name:      "macopy",
			CreatedAt: t.mustTime("2017-06-10 17:30:40"),
		},
		&personRow{
			Name:      "unused",
			CreatedAt: t.mustTime("2017-06-10 18:20:30"),
		},
	}...)
	if err != nil {
		t.Fatalf(`failed to prepare person data: %s`, err)
	}

	err = t.db.Table("test").Create(ctx, []interface{}{
		&testRow{
			ID:       100,
			Data:     "acidlemon-test",
			PersonID: 1,
		},
		&testRow{
			ID:       101,
			Data:     "macopy-test",
			PersonID: 2,
		},
		&testRow{
			ID:       102,
			Data:     "null",
			PersonID: 0,
		},
		&testRow{
			ID:       103,
			Data:     "acidlemon-test2",
			PersonID: 1,
		},
	}...)
	if err != nil {
		t.Fatalf(`failed to prepare test data: %s`, err)
	}

	testRows := []*testRow{}

	// InnerJoin
	{
		rows, err := t.db.Table("test").
			Join("person", "test.person_id = person.id").
			Where("test.id >= 100").
			OrderBy("test.id").All(ctx)
		if err != nil {
			t.Fatalf(`failed to join test & person: %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan testRows: %s`, err)
		}

		if len(testRows) != 3 {
			t.Errorf(`expected row count is 3, but actual %d`, len(testRows))
		}
	}

	// LeftJoin
	{
		rows, err := t.db.Table("test").
			LeftJoin("person", "test.person_id = person.id").
			Where("test.id >= 100").
			OrderBy("test.id").All(ctx)
		if err != nil {
			t.Fatalf(`failed to left join test & person: %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan testRows: %s`, err)
		}

		if len(testRows) != 4 {
			t.Errorf(`expected row count is 4, but actual %d`, len(testRows))
		}
	}

	// RightJoin
	/* Currently sqlite3 does not suppor RIGHT JOIN
	{
		rows, err := t.db.Table("test").
			RightJoin("person", "test.person_id = person.id").
			Where("test.id >= 100").
			OrderBy("test.id").All(ctx)
		if err != nil {
			t.Fatalf(`failed to left join test & person: %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan testRows: %s`, err)
		}

		if len(testRows) != 4 {
			t.Errorf(`expected row count is 4, but actual %d`, len(testRows))
		}
	}
	*/

}

func (t *TestSuite) testWhere() {
	ctx := context.Background()

	// simple WhereEq
	{
		rows, err := t.db.Table("test").WhereEq("data", "acidlemon-test").All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data = "acidlemon-test"): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}
	}

	// WhereLike
	{
		rows, err := t.db.Table("test").WhereLike("data", "acidlemon-test%").All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data like "acidlemon-test%%"): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 2 {
			t.Errorf(`expected testRows count is 2, but actual %d`, len(testRows))
		}
	}

	// WhereIn(simple)
	{
		rows, err := t.db.Table("test").WhereIn("id", 100).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where id in (100)): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}

		rows, err = t.db.Table("test").WhereIn("id", []int{100}).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where id in (100)): %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}
	}

	// WhereIn(complex)
	{
		// 40000 is not exists, expect 3rows
		rows, err := t.db.Table("test").WhereIn("id", 1, 2, 100, 40000).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where id in (1,2,100,40000)): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 3 {
			t.Errorf(`expected testRows count is 3, but actual %d`, len(testRows))
		}

		rows, err = t.db.Table("test").WhereIn("id", []int{1, 2, 100, 40000}).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where id in (1,2,100,40000)): %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 3 {
			t.Errorf(`expected testRows count is 3, but actual %d`, len(testRows))
		}
	}

	// WhereBetween
	{
		// 104, 105 does not exist
		rows, err := t.db.Table("test").WhereBetween("id", 100, 105).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where id between 100 and 105): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 4 {
			t.Errorf(`expected testRows count is 4, but actual %d`, len(testRows))
		}
	}

	// Where(single param)
	{
		rows, err := t.db.Table("test").Where("data = ?", "acidlemon-test").All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data = "acidlemon-test"): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}

		rows, err = t.db.Table("test").Where("data = ?", []interface{}{"acidlemon-test"}).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data = "acidlemon-test"): %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}
	}

	// Where(multi param)
	{
		rows, err := t.db.Table("test").
			Where("data = ? AND id = ?", "acidlemon-test", 100).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data = "acidlemon-test"): %s`, err)
		}

		testRows := []*testRow{}
		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}

		rows, err = t.db.Table("test").
			// Where("data = ? AND id = ?", []string{"acidlemon-test", 100}).All(ctx)
			Where("data = ? AND id = ?", []interface{}{"acidlemon-test", 100}).All(ctx)
		if err != nil {
			t.Fatalf(`failed to get test data (where data = "acidlemon-test"): %s`, err)
		}

		err = rows.Scan(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}
	}

}

func (t *TestSuite) testAggregation() {

}
