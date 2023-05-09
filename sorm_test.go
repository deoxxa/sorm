package sorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTableName(t *testing.T) {
	type MyType struct{ ID string }

	a := assert.New(t)

	a.Equal(TableName(MyType{}), "my_types")
}

func TestTableNameTag(t *testing.T) {
	type MyType struct {
		ID string `table:"alternative_name"`
	}

	a := assert.New(t)

	a.Equal(TableName(MyType{}), "alternative_name")
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

