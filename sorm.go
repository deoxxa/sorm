package sorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/serenize/snaker"
)

func getSQLTableName(t reflect.Type) string {
	for i := 0; i < t.NumField(); i++ {
		if s := t.Field(i).Tag.Get("table"); s != "" {
			return s
		}
	}

	return snaker.CamelToSnake(t.Name()) + "s"
}

func getSQLColumnInfo(f reflect.StructField) (string, []string) {
	a := strings.Split(f.Tag.Get("sql"), ",")
	if a[0] == "" {
		a[0] = snaker.CamelToSnake(f.Name)
	}

	return a[0], a[1:]
}

func getSQLColumnName(f reflect.StructField) string {
	name, _ := getSQLColumnInfo(f)
	return name
}

func arrayHas(a []string, s string) bool {
	for _, e := range a {
		if e == s {
			return true
		}
	}

	return false
}

func getSQLIDFields(t reflect.Type) []string {
	var r []string

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if _, info := getSQLColumnInfo(f); arrayHas(info, "id") {
			r = append(r, f.Name)
		}
	}

	if len(r) == 0 {
		if _, ok := t.FieldByName("ID"); ok {
			r = append(r, "ID")
		}
	}

	return r
}

func TableName(v interface{}) string {
	return getSQLTableName(reflect.TypeOf(v))
}

func ScanRows(rows *sql.Rows, out interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	styp := ptr.Type().Elem()
	if styp.Kind() != reflect.Slice {
		return errors.Errorf("expected output to be pointer to slice; was instead pointer to %s", styp.Kind())
	}

	vtyp := styp.Elem()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("expected output to be pointer to slice of struct; was instead pointer to slice of %s", vtyp.Kind())
	}

	names, err := rows.Columns()
	if err != nil {
		return errors.Wrap(err, "ScanRows")
	}

	indexes := make([]int, len(names))
	missing := make([]string, 0)

outer:
	for i, name := range names {
		for j := 0; j < vtyp.NumField(); j++ {
			f := vtyp.Field(j)

			if a := strings.Split(f.Tag.Get("sql"), ","); a[0] == name {
				indexes[i] = j
				continue outer
			}
		}

		for j := 0; j < vtyp.NumField(); j++ {
			f := vtyp.Field(j)

			if f.Name == name {
				indexes[i] = j
				continue outer
			}
		}

		for j := 0; j < vtyp.NumField(); j++ {
			f := vtyp.Field(j)

			if snaker.CamelToSnake(f.Name) == name {
				indexes[i] = j
				continue outer
			}
		}

		missing = append(missing, name)
	}

	if len(missing) > 0 {
		return errors.Errorf("couldn't find fields on %s for these sql fields: %v", vtyp.Name(), missing)
	}

	arr := reflect.Indirect(reflect.New(styp))

	for rows.Next() {
		v := reflect.New(vtyp).Elem()

		args := make([]interface{}, len(indexes))
		for i, j := range indexes {
			args[i] = v.Field(j).Addr().Interface()
		}

		if err := rows.Scan(args...); err != nil {
			return errors.Wrap(err, "ScanRows")
		}

		arr.Set(reflect.Append(arr, v))
	}

	ptr.Elem().Set(arr)

	return nil
}

type Querier interface {
	ExecContext(ctx context.Context, s string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, s string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, s string, args ...interface{}) *sql.Row
}

func CountWhere(ctx context.Context, db Querier, val interface{}, where string, args ...interface{}) (int, error) {
	ptr := reflect.ValueOf(val)
	if ptr.Kind() != reflect.Ptr {
		return 0, errors.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Type().Elem()
	if vtyp.Kind() != reflect.Struct {
		return 0, errors.Errorf("expected output to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	tbl := getSQLTableName(vtyp)

	if where != "" {
		where = " " + where
	}

	var n int
	if err := db.QueryRowContext(ctx, "select count(*) from "+tbl+where, args...).Scan(&n); err != nil {
		return 0, errors.Wrap(err, "CountWhere")
	}

	return n, nil
}

func CountAll(ctx context.Context, db Querier, val interface{}) (int, error) {
	return CountWhere(ctx, db, val, "")
}

func FindWhere(ctx context.Context, db Querier, out interface{}, where string, args ...interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	styp := ptr.Type().Elem()
	if styp.Kind() != reflect.Slice {
		return errors.Errorf("expected output to be pointer to slice; was instead pointer to %s", styp.Kind())
	}

	vtyp := styp.Elem()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("expected output to be pointer to slice of struct; was instead pointer to slice of %s", vtyp.Kind())
	}

	tbl := getSQLTableName(vtyp)

	if where != "" {
		where = " " + where
	}

	rows, err := db.QueryContext(ctx, "select * from "+tbl+where, args...)
	if err != nil {
		return errors.Wrap(err, "FindWhere")
	}
	defer rows.Close()

	if err := ScanRows(rows, out); err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		return errors.Wrap(err, "FindWhere")
	}

	return nil
}

func FindAll(ctx context.Context, db Querier, out interface{}) error {
	return FindWhere(ctx, db, out, "")
}

func FindFirstWhere(ctx context.Context, db Querier, out interface{}, where string, args ...interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("expected output to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	arr := reflect.New(reflect.SliceOf(vtyp))

	if where != "" {
		where = where + " "
	}

	if err := FindWhere(ctx, db, arr.Interface(), where+"limit 1", args...); err != nil {
		return err
	}

	if arr.Elem().Len() == 0 {
		return sql.ErrNoRows
	}

	ptr.Elem().Set(arr.Elem().Index(0))

	return nil
}

func FindFirst(ctx context.Context, db Querier, out interface{}) error {
	return FindFirstWhere(ctx, db, out, "")
}

type BeforeSaver interface {
	BeforeSave(ctx context.Context, tx *sql.Tx) error
}

func SaveRecordWithTransaction(ctx context.Context, db *sql.DB, input interface{}) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "SaveRecordWithTransaction: couldn't open a transaction")
	}
	defer tx.Rollback()

	if err := SaveRecord(ctx, tx, input); err != nil {
		return errors.Wrap(err, "SaveRecordWithTransaction")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "SaveRecordWithTransaction: couldn't commit transaction")
	}

	return nil
}

func SaveRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeSaver); ok {
		if err := v.BeforeSave(ctx, tx); err != nil {
			return errors.Wrap(err, "SaveRecord: BeforeSave callback returned an error")
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("SaveRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("SaveRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	idFields := getSQLIDFields(vtyp)
	if len(idFields) == 0 {
		return errors.Errorf("SaveRecord: couldn't determine ID field(s)")
	}

	var values []interface{}

	var where string
	for _, fieldName := range idFields {
		f, _ := vtyp.FieldByName(fieldName)

		if where == "" {
			where += "where "
		} else {
			where += " and "
		}

		where += fmt.Sprintf("%s = $%d", getSQLColumnName(f), len(values)+1)
		values = append(values, ptr.Elem().FieldByName(fieldName).Interface())
	}

	previous := reflect.New(vtyp)
	if err := FindFirstWhere(ctx, tx, previous.Interface(), where, values...); err != nil {
		return errors.Wrap(err, "SaveRecord: couldn't find record")
	}

	var fields string
	var modify bool
	for i := 0; i < vtyp.NumField(); i++ {
		f := vtyp.Field(i)

		if f.Tag.Get("readonly") != "" {
			continue
		}

		if reflect.DeepEqual(previous.Elem().Field(i).Interface(), ptr.Elem().Field(i).Interface()) {
			continue
		}

		if fields == "" {
			fields += "set "
		} else {
			fields += ", "
		}

		fields += fmt.Sprintf("%s = $%d", getSQLColumnName(f), len(values)+1)
		values = append(values, ptr.Elem().Field(i).Interface())

		modify = true
	}

	if !modify {
		return nil
	}

	tbl := getSQLTableName(vtyp)

	q := fmt.Sprintf("update %s %s %s", tbl, fields, where)

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return errors.Wrap(err, "SaveRecord")
	}

	return nil
}

type BeforeCreater interface {
	BeforeCreate(ctx context.Context, tx *sql.Tx) error
}

func CreateRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeCreater); ok {
		if err := v.BeforeCreate(ctx, tx); err != nil {
			return errors.Wrap(err, "CreateRecord: BeforeCreate callback returned an error")
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("CreateRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("CreateRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	idFields := getSQLIDFields(vtyp)
	if len(idFields) == 0 {
		return errors.Errorf("CreateRecord: couldn't determine ID field(s)")
	}

	var a1, a2 []string
	var values []interface{}
	var basicID, fetchID bool

	if len(idFields) == 1 && idFields[0] == "ID" {
		basicID = true
	}

	for i := 0; i < vtyp.NumField(); i++ {
		f := vtyp.Field(i)

		if basicID && f.Name == "ID" && isZero(ptr.Elem().Field(i).Interface()) {
			fetchID = true
			continue
		}

		a1 = append(a1, getSQLColumnName(f))
		a2 = append(a2, fmt.Sprintf("$%d", len(a1)))

		values = append(values, ptr.Elem().Field(i).Interface())
	}

	tbl := getSQLTableName(vtyp)

	q := fmt.Sprintf("insert into %s (%s) values (%s)", tbl, strings.Join(a1, ", "), strings.Join(a2, ", "))

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return errors.Wrap(err, "CreateRecord")
	}

	if basicID && fetchID {
		if err := tx.QueryRowContext(ctx, "select last_insert_rowid()").Scan(ptr.Elem().FieldByName("ID").Addr().Interface()); err != nil {
			return errors.Wrap(err, "CreateRecord: couldn't fetch insert id")
		}
	}

	return nil
}

type BeforeReplacer interface {
	BeforeReplace(ctx context.Context, tx *sql.Tx) error
}

func ReplaceRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeReplacer); ok {
		if err := v.BeforeReplace(ctx, tx); err != nil {
			return errors.Wrap(err, "ReplaceRecord: BeforeReplace callback returned an error")
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("ReplaceRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("ReplaceRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	idFields := getSQLIDFields(vtyp)
	if len(idFields) == 0 {
		return errors.Errorf("ReplaceRecord: couldn't determine ID field(s)")
	}

	var a1, a2 []string
	var values []interface{}

	for i := 0; i < vtyp.NumField(); i++ {
		f := vtyp.Field(i)

		a1 = append(a1, getSQLColumnName(f))
		a2 = append(a2, fmt.Sprintf("$%d", len(a1)))

		values = append(values, ptr.Elem().Field(i).Interface())
	}

	tbl := getSQLTableName(vtyp)

	q := fmt.Sprintf("insert or replace into %s (%s) values (%s)", tbl, strings.Join(a1, ", "), strings.Join(a2, ", "))

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return errors.Wrap(err, "ReplaceRecord")
	}

	return nil
}

type BeforeDeleter interface {
	BeforeDelete(ctx context.Context, tx *sql.Tx) error
}

func DeleteRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeDeleter); ok {
		if err := v.BeforeDelete(ctx, tx); err != nil {
			return errors.Wrap(err, "DeleteRecord: BeforeDelete callback returned an error")
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return errors.Errorf("DeleteRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return errors.Errorf("DeleteRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	idFields := getSQLIDFields(vtyp)
	if len(idFields) == 0 {
		return errors.Errorf("DeleteRecord: couldn't determine ID field(s)")
	}

	var values []interface{}

	var where string
	for _, fieldName := range idFields {
		f, _ := vtyp.FieldByName(fieldName)

		if where == "" {
			where += "where "
		} else {
			where += "and "
		}

		where += fmt.Sprintf("%s = $%d", getSQLColumnName(f), len(values)+1)
		values = append(values, ptr.Elem().FieldByName(fieldName).Interface())
	}

	tbl := getSQLTableName(vtyp)

	q := fmt.Sprintf("delete from %s %s", tbl, where)

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return errors.Wrap(err, "DeleteRecord")
	}

	return nil
}

func isZero(i interface{}) bool {
	switch v := i.(type) {
	case []interface{}:
		return len(v) == 0
	case []string:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	case int:
		return v == 0
	case float64:
		return v == 0
	case string:
		return v == ""
	case nil:
		return true
	default:
		return reflect.ValueOf(i).IsZero()
	}
}
