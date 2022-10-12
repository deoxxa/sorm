package sorm

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
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

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectQuery(`select \* from objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r []Object
	a.NoError(FindWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal([]Object{{ID: 1, Name: "test1"}}, r)
}

func TestFindAll(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectQuery(`select \* from objects`).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1").AddRow(2, "test2"))

	var r []Object
	a.NoError(FindAll(context.Background(), db, &r))

	a.Equal([]Object{{ID: 1, Name: "test1"}, {ID: 2, Name: "test2"}}, r)
}

func TestFindFirstWhere(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectQuery(`select \* from objects where id = \$1 limit 1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

	var r Object
	a.NoError(FindFirstWhere(context.Background(), db, &r, "where id = $1", 1))

	a.Equal(Object{ID: 1, Name: "test1"}, r)
}

func TestFindFirst(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectQuery(`select \* from objects limit 1`).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))

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

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`insert into simple_objects \(id, name\) values \(\$1, \$2\)`).WithArgs(1, "test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestCreateRecordEmptyID(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`insert into simple_objects \(name\) values \(\$1\)`).WithArgs("test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`select last_insert_rowid()`).WillReturnRows(sqlmock.NewRows([]string{"?"}).AddRow(1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestCreateRecordCompositeID(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`insert into composite_id_objects \(id_1, id_2, name\) values \(\$1, \$2, \$3\)`).WithArgs(101, 201, "test1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}
	a.NoError(CreateRecord(context.Background(), tx, &r))

	a.Equal(CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSaveRecord(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mock.ExpectExec(`update simple_objects set name = \$2 where id = \$1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1_modified"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

	_ = tx.Commit()
}

func TestSaveRecordNoChange(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSaveRecordCompositeID(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from composite_id_objects where id_1 = \$1 and id_2 = \$2`).WithArgs(101, 201).WillReturnRows(sqlmock.NewRows([]string{"id_1", "id_2", "name"}).AddRow(101, 201, "test1"))
	mock.ExpectExec(`update composite_id_objects set name = \$3 where id_1 = $1 and id_2 = \$2`).WithArgs(101, 201, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(CompositeIDObject{ID1: 101, ID2: 201, Name: "test1"}, r)

	_ = tx.Commit()
}

func TestSetParameterPrefix(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	SetParameterPrefix("?")
	defer func() { SetParameterPrefix("") }()

	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from simple_objects where id = \?1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mock.ExpectExec(`update simple_objects set name = \?2 where id = \?1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()

	r := SimpleObject{ID: 1, Name: "test1_modified"}
	a.NoError(SaveRecord(context.Background(), tx, &r))

	a.Equal(SimpleObject{ID: 1, Name: "test1_modified"}, r)

	_ = tx.Commit()
}

func TestResetParameterPrefix(t *testing.T) {
	a := assert.New(t)

	db, mock, err := sqlmock.New()
	if !a.NoError(err) {
		return
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from simple_objects where id = \?1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mock.ExpectExec(`update simple_objects set name = \?2 where id = \?1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery(`select \* from simple_objects where id = \$1`).WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1"))
	mock.ExpectExec(`update simple_objects set name = \$2 where id = \$1`).WithArgs(1, "test1_modified").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

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
