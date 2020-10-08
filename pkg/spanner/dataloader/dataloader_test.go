package dataloader

import (
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
)

func createRow(t *testing.T, values []interface{}, names []string) *spanner.Row {
	t.Helper()

	// column names are not important in this test, so use dummy name
	if len(names) == 0 {
		names = make([]string, len(values))
		for i := 0; i < len(names); i++ {
			names[i] = "dummy"
		}
	}

	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatalf("Creating spanner row failed unexpectedly: %v", err)
	}
	return row
}

func createColumnValue(t *testing.T, value interface{}) spanner.GenericColumnValue {
	t.Helper()

	row := createRow(t, []interface{}{value}, nil)
	var cv spanner.GenericColumnValue
	if err := row.Column(0, &cv); err != nil {
		t.Fatalf("Creating spanner column value failed unexpectedly: %v", err)
	}

	return cv
}

func equalStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDecodeColumn(t *testing.T) {
	tests := []struct {
		desc       string
		value      interface{}
		want       string
		wantQuoted string
	}{
		// non-nullable
		{
			desc:       "bool",
			value:      true,
			want:       "true",
			wantQuoted: "true",
		},
		{
			desc:       "bytes",
			value:      []byte{'a', 'b', 'c'},
			want:       "YWJj", // base64 encoded 'abc'
			wantQuoted: "YWJj", // base64 encoded 'abc'
		},
		{
			desc:       "float64",
			value:      1.23,
			want:       "1.230000",
			wantQuoted: "1.230000",
		},
		{
			desc:       "int64",
			value:      123,
			want:       "123",
			wantQuoted: "123",
		},
		{
			desc:       "numeric",
			value:      big.NewRat(123, 100),
			want:       "1.23",
			wantQuoted: "1.23",
		},
		{
			desc:       "string",
			value:      "foo",
			want:       "foo",
			wantQuoted: "'foo'",
		},
		{
			desc:       "timestamp",
			value:      time.Unix(1516676400, 0),
			want:       "2018-01-23T03:00:00Z",
			wantQuoted: "'2018-01-23T03:00:00Z'",
		},
		{
			desc:       "date",
			value:      civil.DateOf(time.Unix(1516676400, 0)),
			want:       "2018-01-23",
			wantQuoted: "'2018-01-23'",
		},

		// nullable
		{
			desc:       "null bool",
			value:      spanner.NullBool{Bool: false, Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null bytes",
			value:      []byte(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null float64",
			value:      spanner.NullFloat64{Float64: 0, Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null int64",
			value:      spanner.NullInt64{Int64: 0, Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null numeric",
			value:      spanner.NullNumeric{Numeric: big.Rat{}, Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null string",
			value:      spanner.NullString{StringVal: "", Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null time",
			value:      spanner.NullTime{Time: time.Unix(0, 0), Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null date",
			value:      spanner.NullDate{Date: civil.DateOf(time.Unix(0, 0)), Valid: false},
			want:       "NULL",
			wantQuoted: "NULL",
		},

		// array non-nullable
		{
			desc:       "empty array",
			value:      []bool{},
			want:       "[]",
			wantQuoted: "[]",
		},
		{
			desc:       "array bool",
			value:      []bool{true, false},
			want:       "[true, false]",
			wantQuoted: "[true, false]",
		},
		{
			desc:       "array bytes",
			value:      [][]byte{{'a', 'b', 'c'}, {'e', 'f', 'g'}},
			want:       "[YWJj, ZWZn]",
			wantQuoted: "[YWJj, ZWZn]",
		},
		{
			desc:       "array float64",
			value:      []float64{1.23, 2.45},
			want:       "[1.230000, 2.450000]",
			wantQuoted: "[1.230000, 2.450000]",
		},
		{
			desc:       "array int64",
			value:      []int64{123, 456},
			want:       "[123, 456]",
			wantQuoted: "[123, 456]",
		},
		{
			desc:       "array numeric",
			value:      []*big.Rat{big.NewRat(123, 100), big.NewRat(456, 1)},
			want:       "[1.23, 456]",
			wantQuoted: "[1.23, 456]",
		},
		{
			desc:       "array string",
			value:      []string{"foo", "bar"},
			want:       "[foo, bar]",
			wantQuoted: "['foo', 'bar']",
		},
		{
			desc:       "array timestamp",
			value:      []time.Time{time.Unix(1516676400, 0), time.Unix(1516680000, 0)},
			want:       "[2018-01-23T03:00:00Z, 2018-01-23T04:00:00Z]",
			wantQuoted: "['2018-01-23T03:00:00Z', '2018-01-23T04:00:00Z']",
		},
		{
			desc:       "array date",
			value:      []civil.Date{civil.DateOf(time.Unix(1516676400, 0)), civil.DateOf(time.Unix(1516762800, 0))},
			want:       "[2018-01-23, 2018-01-24]",
			wantQuoted: "['2018-01-23', '2018-01-24']",
		},
		{
			desc: "array struct",
			value: []struct {
				X int64
				Y spanner.NullString
			}{
				{
					X: 10,
					Y: spanner.NullString{StringVal: "Hello", Valid: true},
				},
				{
					X: 20,
					Y: spanner.NullString{StringVal: "", Valid: false},
				},
			},
			want:       "[[10, Hello], [20, NULL]]",
			wantQuoted: "[[10, 'Hello'], [20, NULL]]",
		},

		// array nullable
		{
			desc:       "null array bool",
			value:      []bool(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array bytes",
			value:      [][]byte(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "nul array float64",
			value:      []float64(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array int64",
			value:      []int64(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array numeric",
			value:      []*big.Rat(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array string",
			value:      []string(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array timestamp",
			value:      []time.Time(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
		{
			desc:       "null array date",
			value:      []civil.Date(nil),
			want:       "NULL",
			wantQuoted: "NULL",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			got, err := DecodeColumn(createColumnValue(t, test.value), false)
			if err != nil {
				t.Error(err)
			}
			if got != test.want {
				t.Errorf("DecodeColumn(%v) = %v, want = %v", test.value, got, test.want)
			}
			got, err = DecodeColumn(createColumnValue(t, test.value), true)
			if err != nil {
				t.Error(err)
			}
			if got != test.wantQuoted {
				t.Errorf("DecodeColumn(%v) = %v, wantQuoted = %v", test.value, got, test.wantQuoted)
			}
		})
	}
}

func TestDecodeRow(t *testing.T) {
	tests := []struct {
		desc       string
		values     []interface{}
		want       []string
		wantQuoted []string
	}{
		{
			desc:       "non-null columns",
			values:     []interface{}{"foo", 123},
			want:       []string{"foo", "123"},
			wantQuoted: []string{"'foo'", "123"},
		},
		{
			desc:       "non-null column and null column",
			values:     []interface{}{"foo", spanner.NullString{StringVal: "", Valid: false}},
			want:       []string{"foo", "NULL"},
			wantQuoted: []string{"'foo'", "NULL"},
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			got, err := DecodeRow(createRow(t, test.values, nil), false)
			if err != nil {
				t.Error(err)
			}
			if !equalStringSlice(got, test.want) {
				t.Errorf("DecodeRow(%v) = %v, want = %v", test.values, got, test.want)
			}

			got, err = DecodeRow(createRow(t, test.values, nil), true)
			if err != nil {
				t.Error(err)
			}
			if !equalStringSlice(got, test.wantQuoted) {
				t.Errorf("DecodeRow(%v) = %v, wantQuoted = %v", test.values, got, test.wantQuoted)
			}
		})
	}
}

func TestRowToInsertStatement(t *testing.T) {
	type args struct {
		tableName string
		names     []string
		row       []interface{}
	}
	tests := []struct {
		args    args
		want    string
		wantErr bool
	}{
		{
			args: args{
				"SchemaMigrations",
				[]string{"Version", "Dirty"},
				[]interface{}{1, false},
			},
			want:    "INSERT INTO SchemaMigrations(Version, Dirty) VALUES(1, false);",
			wantErr: false,
		},
		{
			args: args{
				"DatesTable",
				[]string{"Timestamp", "Datetime"},
				[]interface{}{time.Unix(1516676400, 0), civil.DateOf(time.Unix(1516676400, 0))},
			},
			want:    "INSERT INTO DatesTable(Timestamp, Datetime) VALUES('2018-01-23T03:00:00Z', '2018-01-23');",
			wantErr: false,
		},
		{
			args: args{
				"Singers",
				[]string{"SingerID", "FirstName", "LastName"},
				[]interface{}{"1", "Prince", spanner.NullString{}},
			},
			want:    "INSERT INTO Singers(SingerID, FirstName, LastName) VALUES('1', 'Prince', NULL);",
			wantErr: false,
		},
		{
			args: args{
				"SchemaMigrations",
				[]string{"Version", "Dirty"},
				[]interface{}{1, false},
			},
			want:    "INSERT INTO SchemaMigrations(Version, Dirty) VALUES(1, false);",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.args.tableName, func(t *testing.T) {
			got, err := RowToInsertStatement(tt.args.tableName, createRow(t, tt.args.row, tt.args.names))
			if (err != nil) != tt.wantErr {
				t.Errorf("RowToInsertStatement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RowToInsertStatement() got = %v, want %v", got, tt.want)
			}
		})
	}
}
