package aqua

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/k0kubun/pp"
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
	// create test db file name
	now := time.Now()
	dbfile := now.Format("/tmp/hoge-20060102150405.db")

	t.testDB(dbfile)
	t.testCreate()
	t.testJoin()
	t.testWhere()
	t.testSelect()
	t.testAggregation()
	t.testUpdate()
	t.testDelete()
	t.testTx()
	t.testRows()
	t.testMisc()

	os.Remove(dbfile)
}

func (t *TestSuite) testDB(dsn string) {
	ctx := context.Background()

	db, err := Open(t.provider, "sqlite3", dsn)
	if err != nil {
		t.Fatalf(`cannot open database: %s`, err)
	}

	Print(db)

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
		err = row.ScanRow(&r2)
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
		defer rows.Close()

		r2 := []*testRow{}

		err = rows.ScanAll(&r2)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		testRows := []*testRow{}
		err = rows.ScanAll(&testRows)
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
		defer rows.Close()

		err = rows.ScanAll(&testRows)
		if err != nil {
			t.Fatalf(`failed to scan fetched data: %s`, err)
		}

		if len(testRows) != 1 {
			t.Errorf(`expected testRows count is 1, but actual %d`, len(testRows))
		}
	}

}

func (t *TestSuite) testSelect() {
	ctx := context.Background()
	// Select
	{
		row, err := t.db.Table("test").Select("data", "person_id").WhereEq("id", 100).Single(ctx)
		var data string
		var personID int
		err = row.Scan(&data, &personID)
		if err != nil {
			t.Errorf(`failed to scan single row: %s`, err)
		}

		if data != "acidlemon-test" {
			t.Errorf(`expected data is "acidlemon-test", but actual %s`, data)
		}
		if personID != 1 {
			t.Errorf(`expected person_id is 1, but actual %d`, personID)
		}
	}

	// FetchColumn (for select single column & get slice)
	{
		rows, err := t.db.Table("test").Where("id >= 100").FetchColumn(ctx, "data")
		result := []string{}
		err = rows.ScanAll(&result)
		if err != nil {
			t.Errorf(`failed to scan columns: %s`, err)
		}

		expected := []string{
			"acidlemon-test",
			"macopy-test",
			"null",
			"acidlemon-test2",
		}
		if len(result) != 4 {
			t.Errorf(`expected row count is 4, but actual row count is %d`, len(result))
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("expected slice=%v, but actual slice=%v", expected, result)
		}
	}

}

func (t *TestSuite) testAggregation() {

}

func (t *TestSuite) fetchTestRow(ctx context.Context, runner QueryRunner, id int) testRow {
	var r testRow
	row, err := runner.Table("test").WhereEq("id", id).Single(ctx)
	if err != nil {
		t.Fatalf(`failed to get test row (id = %s): %s`, id, err)
	}
	err = row.ScanRow(&r)
	if err != nil {
		t.Fatalf(`failed to scan row: %s`, err)
	}

	return r

}

func (t *TestSuite) testUpdate() {
	ctx := context.Background()

	// update using existing struct
	{
		newData := "updated acidlemon-test"
		r := t.fetchTestRow(ctx, t.db, 100)
		r.Data = newData
		err := t.db.Table("test").Update(ctx, &r)
		if err != nil {
			t.Fatalf(`failed to update row using struct: %s`, err)
		}

		r = t.fetchTestRow(ctx, t.db, 100)
		if r.Data != newData {
			t.Errorf(`Data did not update correctly, actual: %s`, r.Data)
		}
	}

	// update using new struct
	{
		newData := "new-struct acidlemon-test"
		r := testRow{
			Data: newData,
		}
		err := t.db.Table("test").WhereEq("id", 100).Update(ctx, &r)
		if err != nil {
			t.Fatalf(`failed to update row using struct: %s`, err)
		}

		r = t.fetchTestRow(ctx, t.db, 100)
		if r.Data != newData {
			t.Errorf(`Data did not update correctly, actual: %s`, r.Data)
		}
	}

	// update using map
	{
		newData := "map acidlemon-test"
		m := map[string]interface{}{
			"data": newData,
		}
		err := t.db.Table("test").WhereEq("id", 100).Update(ctx, m)
		if err != nil {
			t.Fatalf(`failed to update row using struct: %s`, err)
		}

		r := t.fetchTestRow(ctx, t.db, 100)
		if r.Data != newData {
			t.Errorf(`Data did not update correctly, actual: %s`, r.Data)
		}
	}

}

func (t *TestSuite) testDelete() {
	ctx := context.Background()

	r := t.fetchTestRow(ctx, t.db, 100)
	// delete
	err := t.db.Table("test").Delete(ctx, &r)
	if err != nil {
		t.Fatalf(`failed to delete row: %s`, err)
	}

	r = t.fetchTestRow(ctx, t.db, 100)
	if r.ID == 100 {
		t.Errorf(`row exists, expected result is no row`)
	}

}

func (t *TestSuite) testTx() {
	ctx := context.Background()

	// begin - select - update - commit - select
	{
		newData := "transaction-commit macopy-test"

		tx, err := t.db.Begin(ctx, nil)
		if err != nil {
			t.Fatalf(`failed to begin transaction: %s`, err)
		}

		r := t.fetchTestRow(ctx, tx, 101)
		if r.ID != 101 {
			t.Fatalf(`failed to fetch row...`)
		}
		r.Data = newData

		err = tx.Table("test").Update(ctx, &r)
		if err != nil {
			t.Fatalf(`failed to update row: %s`, err)
		}

		// check row from another connection
		r = t.fetchTestRow(ctx, t.db, 101)
		if r.ID != 101 {
			t.Errorf(`failed to fetch row...`)
		}
		if r.Data == newData {
			t.Errorf(`read dirty data from another connection`)
		}

		err = tx.Commit()
		if err != nil {
			t.Errorf(`failed to commit: %s`, err)
		}

		r = t.fetchTestRow(ctx, t.db, 101)
		if r.ID != 101 {
			t.Errorf(`failed to fetch row...`)
		}
		if r.Data != newData {
			t.Errorf(`read data from another connection is not correct commit data`)
		}
	}

	// begin - select - delete - rollback - select
	{
		tx, err := t.db.Begin(ctx, nil)
		if err != nil {
			t.Fatalf(`failed to begin transaction: %s`, err)
		}

		r := t.fetchTestRow(ctx, tx, 101)
		if r.ID != 101 {
			t.Fatalf(`failed to fetch row...`)
		}
		err = tx.Table("test").Delete(ctx, &r)
		if err != nil {
			t.Fatalf(`failed to delete row: %s`, err)
		}

		// check row from another connection
		r = t.fetchTestRow(ctx, t.db, 101)
		if r.ID == 0 {
			t.Errorf(`row has deleted before commit`)
		}
		if r.ID != 101 {
			t.Errorf(`unexpected row id, expected=101, actual=%d`, r.ID)
		}

		err = tx.Rollback()
		if err != nil {
			t.Errorf(`failed to rollback: %s`, err)
		}

		r = t.fetchTestRow(ctx, t.db, 101)
		if r.ID == 0 {
			t.Errorf(`row has deleted but it must exist due to rollback`)
		}
		if r.ID != 101 {
			t.Errorf(`unexpected row id, expected=101, actual=%d`, r.ID)
		}
	}
}

func (t *TestSuite) testRows() {
	ctx := context.Background()

	rows, err := t.db.Table("test").Where("id >= 100").OrderBy("id DESC").All(ctx)
	if err != nil {
		t.Fatalf(`failed to fetch test.id > 1 rows: %s`, err)
	}
	defer rows.Close()

	// check columns
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf(`failed to get rows' columns: %s`, err)
	}
	expectedCols := []string{"id", "data", "person_id"}
	if !reflect.DeepEqual(columns, expectedCols) {
		t.Errorf(`expected columns are %v, but actual %v`, expectedCols, columns)
	}

	// scan sql.Rows style
	i := 0
	expected := []testRow{{
		ID:       103,
		Data:     "acidlemon-test2",
		PersonID: 1,
	}, {
		ID:       102,
		Data:     "null",
		PersonID: 0,
	}, {
		ID:       101,
		Data:     "transaction-commit macopy-test",
		PersonID: 2,
	}}
	for rows.Next() {

		r := testRow{}
		err := rows.Scan(&r.ID, &r.Data, &r.PersonID)
		if err != nil {
			t.Errorf(`failed to scan: %s`, err)
		}

		if i < len(expected) {
			if expected[i].ID != r.ID {
				t.Errorf(`expected ID is %d, but actual ID is %d`,
					expected[i].ID, r.ID)
			}

			if expected[i].Data != r.Data {
				t.Errorf(`expected Data is %s, but actual Data is %s`,
					expected[i].Data, r.Data)
			}

			if expected[i].PersonID != r.PersonID {
				t.Errorf(`expected PersonID is %d, but actual PersonID is %d`,
					expected[i].PersonID, r.PersonID)
			}
		}

		i++
	}

	if i != 3 {
		t.Errorf(`rows.Scan should call 3 times, but actual %d times`, i)
	}

	if rows.Err() != nil {
		t.Errorf(`something error occured during scan loop: %s`, err)
	}

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
	t.db.Driver()

	// see you again
	err = t.db.Close()
	if err != nil {
		t.Errorf(`something happen on Close: %s`, err)
	}
}
