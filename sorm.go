package sorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"fknsrs.biz/p/reflectutil"
	"github.com/serenize/snaker"
)

var (
	parameterPrefix string
)

func SetParameterPrefix(s string) {
	parameterPrefix = s
}

func makeParameter(n int) string {
	s := parameterPrefix
	if s == "" {
		s = "$"
	}

	return fmt.Sprintf("%s%d", s, n)
}

var (
	descriptionCache = map[reflect.Type]*reflectutil.StructDescription{}
)

func getDescriptionFromType(typ reflect.Type) (*reflectutil.StructDescription, error) {
	if _, ok := descriptionCache[typ]; !ok {
		d, err := reflectutil.GetDescriptionFromType(typ)
		if err != nil {
			return nil, err
		}

		descriptionCache[typ] = d
	}

	return descriptionCache[typ], nil
}

func getSQLTableName(vdesc *reflectutil.StructDescription) string {
	for _, f := range vdesc.Fields() {
		if t := f.Tag("table"); t != nil && t.Value() != "" {
			return t.Value()
		}

		if t := f.Tag("sql"); t != nil {
			if p := t.Parameter("table"); p != nil && p.Value() != "" {
				return p.Value()
			}
		}
	}

	return snaker.CamelToSnake(vdesc.Name()) + "s"
}

func getSQLColumnName(f reflectutil.Field) string {
	if t := f.Tag("sql"); t != nil && t.Value() != "" {
		return t.Value()
	}

	return snaker.CamelToSnake(f.Name())
}

func getSQLIDFields(vdesc *reflectutil.StructDescription) []reflectutil.Field {
	var r []reflectutil.Field

	for _, f := range vdesc.Fields().WithoutTagValue("sql", "-") {
		if t := f.Tag("sql"); t != nil && t.Parameter("id") != nil {
			r = append(r, f)
		}
	}

	if len(r) == 0 {
		if f := vdesc.Field("ID"); f != nil {
			if t := f.Tag("sql"); t == nil || t.Value() != "-" {
				r = append(r, *f)
			}
		}
	}

	return r
}

func TableName(v interface{}) string {
	d, err := getDescriptionFromType(reflect.TypeOf(v))
	if err != nil {
		panic(err)
	}

	return getSQLTableName(d)
}

func ScanRows(rows *sql.Rows, out interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	styp := ptr.Type().Elem()
	if styp.Kind() != reflect.Slice {
		return fmt.Errorf("expected output to be pointer to slice; was instead pointer to %s", styp.Kind())
	}

	vtyp := styp.Elem()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("expected output to be pointer to slice of struct; was instead pointer to slice of %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	names, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("ScanRows: %w", err)
	}

	indexes := make([][]int, len(names))
	missing := make([]string, 0)

outer:
	for i, name := range names {
		if l := vdesc.Fields().WithTagValue("sql", name); len(l) == 1 {
			indexes[i] = l[0].Index()
			continue outer
		}

		if f := vdesc.Field(name); f != nil {
			indexes[i] = f.Index()
			continue outer
		}

		for _, f := range vdesc.Fields() {
			if snaker.CamelToSnake(f.Name()) == name {
				indexes[i] = f.Index()
				continue outer
			}
		}

		missing = append(missing, name)
	}

	if len(missing) > 0 {
		return fmt.Errorf("couldn't find fields on %s for these sql fields: %v", vtyp.Name(), missing)
	}

	arr := reflect.Indirect(reflect.New(styp))

	for rows.Next() {
		v := reflect.New(vtyp).Elem()

		args := make([]interface{}, len(indexes))
		for i, index := range indexes {
			args[i] = v.FieldByIndex(index).Addr().Interface()
		}

		if err := rows.Scan(args...); err != nil {
			return fmt.Errorf("ScanRows: %w", err)
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
		return 0, fmt.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Type().Elem()
	if vtyp.Kind() != reflect.Struct {
		return 0, fmt.Errorf("expected output to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return 0, fmt.Errorf("CountWhere: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	tbl := getSQLTableName(vdesc)

	if where != "" {
		where = " " + where
	}

	var n int
	if err := db.QueryRowContext(ctx, "select count(*) from "+tbl+where, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("CountWhere: %w", err)
	}

	return n, nil
}

func CountAll(ctx context.Context, db Querier, val interface{}) (int, error) {
	return CountWhere(ctx, db, val, "")
}

func FindWhere(ctx context.Context, db Querier, out interface{}, where string, args ...interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	styp := ptr.Type().Elem()
	if styp.Kind() != reflect.Slice {
		return fmt.Errorf("expected output to be pointer to slice; was instead pointer to %s", styp.Kind())
	}

	vtyp := styp.Elem()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("expected output to be pointer to slice of struct; was instead pointer to slice of %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("FindWhere: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	tbl := getSQLTableName(vdesc)

	if where != "" {
		where = " " + where
	}

	rows, err := db.QueryContext(ctx, "select * from "+tbl+where, args...)
	if err != nil {
		return fmt.Errorf("FindWhere: %w", err)
	}
	defer rows.Close()

	if err := ScanRows(rows, out); err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		return fmt.Errorf("FindWhere: %w", err)
	}

	return nil
}

func FindAll(ctx context.Context, db Querier, out interface{}) error {
	return FindWhere(ctx, db, out, "")
}

func FindFirstWhere(ctx context.Context, db Querier, out interface{}, where string, args ...interface{}) error {
	ptr := reflect.ValueOf(out)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("expected output to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("expected output to be pointer to struct; was instead pointer to %s", vtyp.Kind())
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

type AfterSaver interface {
	AfterSave(ctx context.Context, tx *sql.Tx) error
}

func SaveRecordWithTransaction(ctx context.Context, db *sql.DB, input interface{}) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("SaveRecordWithTransaction: couldn't open a transaction: %w", err)
	}
	defer tx.Rollback()

	if err := SaveRecord(ctx, tx, input); err != nil {
		return fmt.Errorf("SaveRecordWithTransaction: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("SaveRecordWithTransaction: couldn't commit transaction: %w", err)
	}

	return nil
}

func SaveRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeSaver); ok {
		if err := v.BeforeSave(ctx, tx); err != nil {
			return fmt.Errorf("SaveRecord: BeforeSave callback returned an error: %w", err)
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("SaveRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("SaveRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("SaveRecord: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	idFields := getSQLIDFields(vdesc)
	if len(idFields) == 0 {
		return fmt.Errorf("SaveRecord: couldn't determine ID field(s)")
	}

	var values []interface{}

	var where string
	for _, idField := range idFields {
		if where == "" {
			where += "where "
		} else {
			where += " and "
		}

		where += getSQLColumnName(idField) + " = " + makeParameter(len(values)+1)
		values = append(values, ptr.Elem().FieldByIndex(idField.Index()).Interface())
	}

	previous := reflect.New(vtyp)
	if err := FindFirstWhere(ctx, tx, previous.Interface(), where, values...); err != nil {
		return fmt.Errorf("SaveRecord: couldn't find record: %w", err)
	}

	var fields string
	var modify bool
	for _, f := range vdesc.Fields().WithoutTagValue("sql", "-") {
		if t := f.Tag("sql"); t != nil && t.Parameter("readonly") != nil {
			continue
		}

		if t := f.Tag("readonly"); t != nil && t.Value() != "" {
			continue
		}

		if reflect.DeepEqual(previous.Elem().FieldByIndex(f.Index()).Interface(), ptr.Elem().FieldByIndex(f.Index()).Interface()) {
			continue
		}

		if fields == "" {
			fields += "set "
		} else {
			fields += ", "
		}

		fields += getSQLColumnName(f) + " = " + makeParameter(len(values)+1)
		values = append(values, ptr.Elem().FieldByIndex(f.Index()).Interface())

		modify = true
	}

	if !modify {
		return nil
	}

	tbl := getSQLTableName(vdesc)

	q := fmt.Sprintf("update %s %s %s", tbl, fields, where)

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return fmt.Errorf("SaveRecord: %w", err)
	}

	if v, ok := input.(AfterSaver); ok {
		if err := v.AfterSave(ctx, tx); err != nil {
			return fmt.Errorf("SaveRecord: AfterSave callback returned an error: %w", err)
		}
	}

	return nil
}

type BeforeCreater interface {
	BeforeCreate(ctx context.Context, tx *sql.Tx) error
}

type AfterCreater interface {
	AfterCreate(ctx context.Context, tx *sql.Tx) error
}

func CreateRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeCreater); ok {
		if err := v.BeforeCreate(ctx, tx); err != nil {
			return fmt.Errorf("CreateRecord: BeforeCreate callback returned an error: %w", err)
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("CreateRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("CreateRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("CreateRecord: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	idFields := getSQLIDFields(vdesc)
	if len(idFields) == 0 {
		return fmt.Errorf("CreateRecord: couldn't determine ID field(s)")
	}

	var a1, a2 []string
	var values []interface{}
	var basicID, fetchID bool

	if len(idFields) == 1 && idFields[0].Name() == "ID" {
		basicID = true
	}

	for _, f := range vdesc.Fields().WithoutTagValue("sql", "-") {
		if basicID && f.Name() == "ID" && isZero(ptr.Elem().FieldByIndex(f.Index()).Interface()) {
			fetchID = true
			continue
		}

		a1 = append(a1, getSQLColumnName(f))
		a2 = append(a2, makeParameter(len(a1)))

		values = append(values, ptr.Elem().FieldByIndex(f.Index()).Interface())
	}

	tbl := getSQLTableName(vdesc)

	q := fmt.Sprintf("insert into %s (%s) values (%s)", tbl, strings.Join(a1, ", "), strings.Join(a2, ", "))

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return fmt.Errorf("CreateRecord: %w", err)
	}

	if basicID && fetchID {
		if err := tx.QueryRowContext(ctx, "select last_insert_rowid()").Scan(ptr.Elem().FieldByName("ID").Addr().Interface()); err != nil {
			return fmt.Errorf("CreateRecord: couldn't fetch insert id: %w", err)
		}
	}

	if v, ok := input.(AfterCreater); ok {
		if err := v.AfterCreate(ctx, tx); err != nil {
			return fmt.Errorf("CreateRecord: AfterCreate callback returned an error: %w", err)
		}
	}

	return nil
}

type BeforeReplacer interface {
	BeforeReplace(ctx context.Context, tx *sql.Tx) error
}

type AfterReplacer interface {
	AfterReplace(ctx context.Context, tx *sql.Tx) error
}

func ReplaceRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeReplacer); ok {
		if err := v.BeforeReplace(ctx, tx); err != nil {
			return fmt.Errorf("ReplaceRecord: BeforeReplace callback returned an error: %w", err)
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("ReplaceRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("ReplaceRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("ReplaceRecord: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	idFields := getSQLIDFields(vdesc)
	if len(idFields) == 0 {
		return fmt.Errorf("ReplaceRecord: couldn't determine ID field(s)")
	}

	var a1, a2 []string
	var values []interface{}

	for _, f := range vdesc.Fields().WithoutTagValue("sql", "-") {
		a1 = append(a1, getSQLColumnName(f))
		a2 = append(a2, makeParameter(len(a1)))

		values = append(values, ptr.Elem().FieldByIndex(f.Index()).Interface())
	}

	tbl := getSQLTableName(vdesc)

	q := fmt.Sprintf("insert or replace into %s (%s) values (%s)", tbl, strings.Join(a1, ", "), strings.Join(a2, ", "))

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return fmt.Errorf("ReplaceRecord: %w", err)
	}

	if v, ok := input.(AfterReplacer); ok {
		if err := v.AfterReplace(ctx, tx); err != nil {
			return fmt.Errorf("ReplaceRecord: AfterReplace callback returned an error: %w", err)
		}
	}

	return nil
}

type BeforeDeleter interface {
	BeforeDelete(ctx context.Context, tx *sql.Tx) error
}

type AfterDeleter interface {
	AfterDelete(ctx context.Context, tx *sql.Tx) error
}

func DeleteRecord(ctx context.Context, tx *sql.Tx, input interface{}) error {
	if v, ok := input.(BeforeDeleter); ok {
		if err := v.BeforeDelete(ctx, tx); err != nil {
			return fmt.Errorf("DeleteRecord: BeforeDelete callback returned an error: %w", err)
		}
	}

	ptr := reflect.ValueOf(input)
	if ptr.Kind() != reflect.Ptr {
		return fmt.Errorf("DeleteRecord: expected input to be a pointer; was instead %s", ptr.Kind())
	}

	vtyp := ptr.Elem().Type()
	if vtyp.Kind() != reflect.Struct {
		return fmt.Errorf("DeleteRecord: expected input to be pointer to struct; was instead pointer to %s", vtyp.Kind())
	}

	vdesc, err := getDescriptionFromType(vtyp)
	if err != nil {
		return fmt.Errorf("DeleteRecord: could not get detailed reflection information for type %s: %w", vtyp.String(), err)
	}

	idFields := getSQLIDFields(vdesc)
	if len(idFields) == 0 {
		return fmt.Errorf("DeleteRecord: couldn't determine ID field(s)")
	}

	var values []interface{}

	var where string
	for _, f := range idFields {
		if where == "" {
			where += "where "
		} else {
			where += "and "
		}

		where += getSQLColumnName(f) + " = " + makeParameter(len(values)+1)
		values = append(values, ptr.Elem().FieldByIndex(f.Index()).Interface())
	}

	tbl := getSQLTableName(vdesc)

	q := fmt.Sprintf("delete from %s %s", tbl, where)

	if _, err := tx.ExecContext(ctx, q, values...); err != nil {
		return fmt.Errorf("DeleteRecord: %w", err)
	}

	if v, ok := input.(AfterDeleter); ok {
		if err := v.AfterDelete(ctx, tx); err != nil {
			return fmt.Errorf("DeleteRecord: AfterDelete callback returned an error: %w", err)
		}
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
