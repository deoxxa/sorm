package sorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTableName(t *testing.T) {
	type MyType struct{ ID string }

	a := assert.New(t)

	a.Equal(TableName(MyType{}), "my_types")
}

func BenchmarkTableName(b *testing.B) {
	type MyType struct{ ID string }

	for i := 0; i < b.N; i++ {
		TableName(MyType{})
	}
}

func TestTableNameTag(t *testing.T) {
	type MyType struct {
		ID string `table:"alternative_name"`
	}

	a := assert.New(t)

	a.Equal(TableName(MyType{}), "alternative_name")
}

func BenchmarkTableNameTag(b *testing.B) {
	type MyType struct {
		ID string `table:"alternative_name"`
	}

	for i := 0; i < b.N; i++ {
		TableName(MyType{})
	}
}

type Object struct {
	ID   int
	Name string
}

func TestFindWhere(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r []Object
	a.NoError(FindWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal([]Object{{ID: 1, Name: "test1"}}, r)
}

func TestFindAll(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects`).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1").AddRow(2, "test2"))

	var r []Object
	a.NoError(FindAll(context.Background(), db, &r))

	a.Equal([]Object{{ID: 1, Name: "test1"}, {ID: 2, Name: "test2"}}, r)
}

func TestFindFirstWhere(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects where id = \$1 limit 1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r Object
	a.NoError(FindFirstWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal(Object{ID: 1, Name: "test1"}, r)
}

func TestFindFirst(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects limit 1`).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r Object
	a.NoError(FindFirst(context.Background(), db, &r))

	a.Equal(Object{ID: 1, Name: "test1"}, r)
}

type SimpleObject struct {
	ID   int
	Name string
}

type CompositeIDObject struct {
	ID1  int `sql:",id"`
	ID2  int `sql:",id"`
	Name string
}

func TestCreateRecord(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into simple_objects \(id, name\) values \(\$1, \$2\)`).WithArgs(1, "test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestCreateRecordEmptyID(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into simple_objects \(name\) values \(\$1\)`).WithArgs("test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestCreateRecordCompositeID(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into composite_id_objects \(id_1, id_2, name\) values \(\$1, \$2, \$3\)`).WithArgs(101, 201, "test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSaveRecord(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mockDB.ExpectExec(`update simple_objects set name = \$2 where id = \$1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1_modified"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

	_ = tx.Commit()
}

func TestSaveRecordNoChange(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSaveRecordCompositeID(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from composite_id_objects where id_1 = \$1 and id_2 = \$2`).WithArgs(101, 201).WillReturnRows(sqlmock.NewRows([]string{"id_1", "id_2", "name"}).AddRow(101, 201, "test1"))
	mockDB.ExpectExec(`update composite_id_objects set name = \$3 where id_1 = $1 and id_2 = \$2`).WithArgs(101, 201, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSetParameterPrefix(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	SetParameterPrefix("?")
	defer func() { SetParameterPrefix("") }()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from simple_objects where id = \?1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mockDB.ExpectExec(`update simple_objects set name = \?2 where id = \?1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1_modified"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

	_ = tx.Commit()
}

func TestResetParameterPrefix(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from simple_objects where id = \?1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mockDB.ExpectExec(`update simple_objects set name = \?2 where id = \?1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()
	mockDB.ExpectBegin()
	mockDB.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mockDB.ExpectExec(`update simple_objects set name = \$2 where id = \$1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mockDB.ExpectCommit()

	SetParameterPrefix("?")
	defer func() { SetParameterPrefix("") }()

	{
		tx, _ := db.Begin()

		r := SimpleObject{ID: 1, Name: "test1_modified"}
		a.NoError(SaveRecord(context.Background(), tx, &r))

		a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

		_ = tx.Commit()
	}

	SetParameterPrefix("")

	{
		tx, _ := db.Begin()

		r := SimpleObject{ID: 1, Name: "test1_modified"}
		a.NoError(SaveRecord(context.Background(), tx, &r))

		a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

		_ = tx.Commit()
	}
}

type TestBeforeCreateObject struct {
	m *mock.Mock `sql:"-"`

	ID   int
	Name string
}

func (t *TestBeforeCreateObject) BeforeCreate(ctx context.Context, tx *sql.Tx) error {
	return t.m.MethodCalled("BeforeCreate", ctx, tx).Error(0)
}

func TestBeforeCreateSuccess(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into test_before_create_objects \(name\) values \(\$1\)`).WithArgs("a").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mockDB.ExpectCommit()

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)

	m := &mock.Mock{}
	m.On("BeforeCreate", ctx, tx).Return(error(nil))

	r := TestBeforeCreateObject{m: m, Name: "a"}
	a.NoError(CreateRecord(ctx, tx, &r))

	a.Equal(1, r.ID)
	a.Equal("a", r.Name)

	a.NoError(tx.Commit())

	m.AssertExpectations(t)
}

func TestBeforeCreateError(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into test_before_create_objects \(name\) values \(\$1\)`).WithArgs("a").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mockDB.ExpectCommit()

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)

	testErr := fmt.Errorf("test error")

	m := &mock.Mock{}
	m.On("BeforeCreate", ctx, tx).Return(testErr)

	r := TestBeforeCreateObject{m: m, Name: "a"}
	a.EqualError(CreateRecord(ctx, tx, &r), "CreateRecord: BeforeCreate callback returned an error: test error")

	m.AssertExpectations(t)
}

type TestAfterCreateObject struct {
	m *mock.Mock `sql:"-"`

	ID   int
	Name string
}

func (t *TestAfterCreateObject) AfterCreate(ctx context.Context, tx *sql.Tx) error {
	return t.m.MethodCalled("AfterCreate", ctx, tx).Error(0)
}

func TestAfterCreateSuccess(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into test_after_create_objects \(name\) values \(\$1\)`).WithArgs("a").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mockDB.ExpectCommit()

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)

	m := &mock.Mock{}
	m.On("AfterCreate", ctx, tx).Return(error(nil))

	r := TestAfterCreateObject{m: m, Name: "a"}
	a.NoError(CreateRecord(ctx, tx, &r))

	a.Equal(1, r.ID)
	a.Equal("a", r.Name)

	a.NoError(tx.Commit())

	m.AssertExpectations(t)
}

func TestAfterCreateError(t *testing.T) {
	a := assert.New(t)

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectBegin()
	mockDB.ExpectExec(`insert into test_after_create_objects \(name\) values \(\$1\)`).WithArgs("a").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mockDB.ExpectCommit()

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)

	testErr := fmt.Errorf("test error")

	m := &mock.Mock{}
	m.On("AfterCreate", ctx, tx).Return(testErr)

	r := TestAfterCreateObject{m: m, Name: "a"}
	a.EqualError(CreateRecord(ctx, tx, &r), "CreateRecord: AfterCreate callback returned an error: test error")

	m.AssertExpectations(t)
}

func BenchmarkFindAllLargeResultSets(b *testing.B) {
	type TestObject struct {
		ID   int    `sql:"id"`
		Name string `sql:"name"`
	}

	db := sql.OpenDB(&MockConnector{
		driver: &MockDriver{
			columns: []string{"id", "name"},
			results: b.N,
			fillRow: func(current, total int, values []driver.Value) error {
				if current >= total {
					return io.EOF
				}

				values[0] = int64(current)
				values[1] = "name"

				return nil
			},
		},
	})

	var a []TestObject
	if err := FindAll(context.Background(), db, &a); err != nil {
		panic(err)
	}
}

func TestFindAllLargeResultSets(t *testing.T) {
	type TestObject struct {
		ID   int    `sql:"id"`
		Name string `sql:"name"`
	}

	for _, count := range []int{1, 10, 100, 1000, 5000, 10000, 100000, 1000000} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			a := assert.New(t)

			db := sql.OpenDB(&MockConnector{
				driver: &MockDriver{
					columns: []string{"id", "name"},
					results: count,
					fillRow: func(current, total int, values []driver.Value) error {
						if current >= total {
							return io.EOF
						}

						values[0] = int64(current)
						values[1] = fmt.Sprintf("row_%08d", current)

						return nil
					},
				},
			})

			var l []TestObject
			a.NoError(FindAll(context.Background(), db, &l))
			a.Len(l, count)

			a.Equal(TestObject{0, "row_00000000"}, l[0])
			a.Equal(TestObject{count - 1, fmt.Sprintf("row_%08d", count-1)}, l[count-1])
		})
	}
}

type goodScanner struct{ Value string }

func (s *goodScanner) Scan(src interface{}) error {
	if src, ok := src.(string); ok {
		s.Value = src
		return nil
	}

	return fmt.Errorf("goodScanner: unexpected value type: %T", src)
}

type badScanner struct{ Value string }

func (s *badScanner) Scan(src interface{}) error {
	return fmt.Errorf("badScanner: intentional error")
}

type overrideScanner struct{ Value *badScanner }

func (s *overrideScanner) Scan(src interface{}) error {
	if src, ok := src.(string); ok {
		s.Value.Value = src
		return nil
	}

	return fmt.Errorf("overrideScanner: unexpected value type: %T", src)
}

type testScannersNoScanner struct {
	ID    int `sql:",table:objects"`
	Value string
}

type testScannersGoodScanner struct {
	ID    int `sql:",table:objects"`
	Value goodScanner
}

type testScannersBadScanner struct {
	ID    int `sql:",table:objects"`
	Value badScanner
}

type testScannersGoodOverride struct {
	ID    int `sql:",table:objects"`
	Value badScanner
}

func (o *testScannersGoodOverride) OverrideScan(names []string, scanners []sql.Scanner) error {
	for i, name := range names {
		switch name {
		case "ID":
			// nothing
		case "Value":
			scanners[i] = &overrideScanner{Value: &o.Value}
		default:
			return fmt.Errorf("testScannersGoodOverride: field %d (%s) unrecognised", i, name)
		}
	}

	return nil
}

type testScannersBadOverride struct {
	ID    int `sql:",table:objects"`
	Value badScanner
}

func (o *testScannersBadOverride) OverrideScan(names []string, scanners []sql.Scanner) error {
	return fmt.Errorf("testScannersBadOverride: intentional error")
}

func TestOverrideScanner(t *testing.T) {
	for _, e := range []struct {
		name          string
		err           string
		out, expected interface{}
	}{
		{"no scanner", "", &testScannersNoScanner{}, &testScannersNoScanner{1, "success"}},
		{"good scanner", "", &testScannersGoodScanner{}, &testScannersGoodScanner{1, goodScanner{"success"}}},
		{"bad scanner", "badScanner: intentional error", &testScannersBadScanner{}, &testScannersBadScanner{}},
		{"good override", "", &testScannersGoodOverride{}, &testScannersGoodOverride{1, badScanner{"success"}}},
		{"bad override", "testScannersBadOverride: intentional error", &testScannersBadOverride{}, &testScannersBadOverride{}},
	} {
		t.Run(e.name, func(t *testing.T) {
			a := assert.New(t)

			db, mockDB, err := sqlmock.New()
			if !a.NoError(err) {
				return
			}
			defer db.Close()

			mockDB.ExpectQuery(`select \* from objects limit 1`).WillReturnRows(sqlmock.NewRows([]string{"id", "value"}).AddRow(1, "success"))

			err = FindFirst(context.Background(), db, e.out)
			if e.err != "" {
				a.Error(err)
				a.ErrorContains(err, e.err)
				a.Equal(e.out, e.expected)
			} else {
				a.NoError(err)
				a.Equal(e.out, e.expected)
			}
		})
	}
}

func BenchmarkOverrideScanner(b *testing.B) {
	for _, e := range []struct {
		name string
		fail bool
		out  func() interface{}
	}{
		{"no scanner", false, func() interface{} { return &[]testScannersNoScanner{} }},
		{"good scanner", false, func() interface{} { return &[]testScannersGoodScanner{} }},
		{"bad scanner", true, func() interface{} { return &[]testScannersBadScanner{} }},
		{"good override", false, func() interface{} { return &[]testScannersGoodOverride{} }},
		{"bad override", true, func() interface{} { return &[]testScannersBadOverride{} }},
	} {
		b.Run(e.name, func(b *testing.B) {
			a := assert.New(b)

			db := sql.OpenDB(&MockConnector{
				driver: &MockDriver{
					columns: []string{"id", "value"},
					results: b.N,
					fillRow: func(current, total int, values []driver.Value) error {
						if current >= total {
							return io.EOF
						}

						values[0] = int64(current)
						values[1] = "success"

						return nil
					},
				},
			})

			err := FindAll(context.Background(), db, e.out())
			if e.fail {
				a.Error(err)
			} else {
				a.NoError(err)
			}
		})
	}
}

var ErrUnimplemented = fmt.Errorf("unimplemented")

type MockFillRowFunc func(current, total int, values []driver.Value) error

type MockDriver struct {
	columns []string
	results int
	fillRow MockFillRowFunc
}

func (d *MockDriver) Open(name string) (driver.Conn, error) {
	return &MockConn{driver: d}, nil
}

type MockConnector struct {
	driver *MockDriver
}

func (c *MockConnector) Driver() driver.Driver {
	return c.driver
}

func (c *MockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &MockConn{driver: c.driver}, nil
}

type MockConn struct{ driver *MockDriver }

func (c *MockConn) Prepare(query string) (driver.Stmt, error) {
	return &MockStmt{driver: c.driver}, nil
}

func (c *MockConn) Close() error {
	return nil
}

func (c *MockConn) Begin() (driver.Tx, error) {
	return nil, ErrUnimplemented
}

func (c *MockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrUnimplemented
}

type MockStmt struct{ driver *MockDriver }

func (s *MockStmt) Close() error {
	return nil
}

func (s *MockStmt) NumInput() int {
	return 0
}

func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &MockResult{}, nil
}

func (s *MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &MockRows{driver: s.driver}, nil
}

type MockResult struct{}

func (r *MockResult) LastInsertId() (int64, error) {
	return 0, ErrUnimplemented
}

func (r *MockResult) RowsAffected() (int64, error) {
	return 0, ErrUnimplemented
}

type MockRows struct {
	driver  *MockDriver
	counter int
}

func (r *MockRows) Columns() []string {
	return r.driver.columns
}

func (r *MockRows) Close() error {
	return nil
}

func (r *MockRows) Next(values []driver.Value) error {
	if err := r.driver.fillRow(r.counter, r.driver.results, values); err != nil {
		return err
	}

	r.counter++

	return nil
}

type simpleQueryLog struct {
	phase    string
	query    string
	args     []interface{}
	duration time.Duration
	err      error
}

type simpleQueryLogger struct {
	logs []simpleQueryLog
}

func (s *simpleQueryLogger) LogQuery(query string, args []interface{}) {
	s.logs = append(s.logs, simpleQueryLog{phase: "before", query: query, args: args})
}

func (s *simpleQueryLogger) LogQueryAfter(query string, args []interface{}, duration time.Duration, err error) {
	s.logs = append(s.logs, simpleQueryLog{phase: "after", query: query, args: args, duration: duration.Round(time.Second), err: err})
}

func TestQueryLogger(t *testing.T) {
	a := assert.New(t)

	var logger simpleQueryLogger

	SetQueryLogger(&logger)
	defer func() { SetQueryLogger(nil) }()

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r []Object
	a.NoError(FindWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal([]Object{{ID: 1, Name: "test1"}}, r)

	a.Equal([]simpleQueryLog{
		{phase: "before", query: "select * from objects where id = $1", args: []interface{}{1}},
		{phase: "after", query: "select * from objects where id = $1", args: []interface{}{1}, duration: 0, err: nil},
	}, logger.logs)
}

func TestQueryLoggerFunc(t *testing.T) {
	a := assert.New(t)

	var logs []simpleQueryLog

	SetQueryLoggerFunc(func(query string, args []interface{}) {
		logs = append(logs, simpleQueryLog{query: query, args: args})
	})
	defer func() { SetQueryLogger(nil) }()

	db, mockDB, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mockDB.ExpectQuery(`select \* from objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r []Object
	a.NoError(FindWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal([]Object{{ID: 1, Name: "test1"}}, r)

	a.Equal([]simpleQueryLog{
		{query: "select * from objects where id = $1", args: []interface{}{1}},
	}, logs)
}
