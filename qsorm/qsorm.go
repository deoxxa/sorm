package qsorm

import (
	"context"

	"fknsrs.biz/p/sorm"
	"fknsrs.biz/p/sqlbuilder"
)

var dialect sqlbuilder.Dialect = sqlbuilder.DialectGeneric{}

func SetDialect(d sqlbuilder.Dialect) { dialect = d }

func CountWhere(ctx context.Context, db sorm.Querier, out interface{}, where sqlbuilder.AsExpr) (int, error) {
	s := sqlbuilder.NewSerializer(dialect)

	if where != nil {
		s = s.D("where ").F(where.AsExpr).D(" ")
	}

	qs, qv, err := s.ToSQL()
	if err != nil {
		return 0, err
	}

	return sorm.CountWhere(ctx, db, out, qs, qv...)
}

func FindWhere(ctx context.Context, db sorm.Querier, out interface{}, where sqlbuilder.AsExpr, order []sqlbuilder.AsOrderingTerm, offsetLimit sqlbuilder.AsOffsetLimit) error {
	s := sqlbuilder.NewSerializer(dialect)

	if where != nil {
		s = s.D("where ").F(where.AsExpr).D(" ")
	}
	for i, e := range order {
		s = s.DC("order by ", i == 0).DC(", ", i != 0).F(e.AsOrderingTerm).D(" ")
	}
	if offsetLimit != nil {
		s = s.F(offsetLimit.AsOffsetLimit)
	}

	qs, qv, err := s.ToSQL()
	if err != nil {
		return err
	}

	return sorm.FindWhere(ctx, db, out, qs, qv...)
}

func FindFirstWhere(ctx context.Context, db sorm.Querier, out interface{}, where sqlbuilder.AsExpr, order []sqlbuilder.AsOrderingTerm) error {
	s := sqlbuilder.NewSerializer(dialect)

	if where != nil {
		s = s.D("where ").F(where.AsExpr)
	}
	for i, e := range order {
		s = s.DC("order by ", i == 0).DC(", ", i != 0).F(e.AsOrderingTerm)
	}

	qs, qv, err := s.ToSQL()
	if err != nil {
		return err
	}

	return sorm.FindFirstWhere(ctx, db, out, qs, qv...)
}
