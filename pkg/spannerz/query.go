package spannerz

import (
	"context"

	"cloud.google.com/go/spanner"
)

type Queryer interface {
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

func ReadColumnSQL[T any](ctx context.Context, client Queryer, sql string) (T, error) {
	return ReadColumn[T](ctx, client, spanner.NewStatement(sql))
}

// ReadColumn will execute the stmt and return the first column
func ReadColumn[T any](ctx context.Context, client Queryer, stmt spanner.Statement) (T, error) {
	var col T
	err := client.Query(ctx, stmt).Do(func(r *spanner.Row) error {
		return r.Column(0, &col)
	})
	return col, err
}

func GetSQL[T any](ctx context.Context, client Queryer, sql string) ([]T, error) {
	return Get[T](ctx, client, spanner.NewStatement(sql))
}

func Get[T any](ctx context.Context, client Queryer, stmt spanner.Statement) ([]T, error) {
	var rows []T

	err := client.Query(ctx, stmt).Do(func(r *spanner.Row) error {
		var row T
		if err := r.ToStructLenient(&row); err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return rows, nil
}
