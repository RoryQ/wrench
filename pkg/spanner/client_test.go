// Copyright (c) 2020 Mercari, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package spanner

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/roryq/wrench/pkg/spannerz"
)

const (
	singerTable    = "Singers"
	migrationTable = "SchemaMigrations"
)

type (
	column struct {
		ColumnName  string `spanner:"column_name"`
		SpannerType string `spanner:"spanner_type"`
		IsNullable  string `spanner:"is_nullable"`
	}

	singer struct {
		SingerID  string
		FirstName string
	}

	migration struct {
		Version int64
		Dirty   bool
	}
)

const (
	envSpannerProjectID  = "SPANNER_PROJECT_ID"
	envSpannerInstanceID = "SPANNER_INSTANCE_ID"
	envSpannerDatabaseID = "SPANNER_DATABASE_ID"
)

func TestLoadDDL(t *testing.T) {
	ctx := context.Background()

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	gotDDL, err := client.LoadDDL(ctx)
	if err != nil {
		t.Fatalf("failed to load ddl: %v", err)
	}

	wantDDL, err := ioutil.ReadFile("testdata/schema.sql")
	if err != nil {
		t.Fatalf("failed to read ddl file: %v", err)
	}

	if want, got := string(wantDDL), string(gotDDL); want != got {
		t.Errorf("want: \n%s\n but got: \n%s", want, got)
	}
}

func TestApplyDDLFile(t *testing.T) {
	ctx := context.Background()

	ddl, err := ioutil.ReadFile("testdata/ddl.sql")
	if err != nil {
		t.Fatalf("failed to read ddl file: %v", err)
	}

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	if err := client.ApplyDDLFile(ctx, ddl); err != nil {
		t.Fatalf("failed to apply ddl file: %v", err)
	}

	ri := client.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT column_name, spanner_type FROM information_schema.columns WHERE table_catalog = '' AND table_name = @table AND column_name = @column",
		Params: map[string]interface{}{
			"table":  singerTable,
			"column": "Foo",
		},
	})
	defer ri.Stop()

	row, err := ri.Next()
	if err == iterator.Done {
		t.Fatalf("failed to get table information: %v", err)
	}

	c := &column{}
	if err := row.ToStruct(c); err != nil {
		t.Fatalf("failed to convert row to struct: %v", err)
	}

	if want, got := "Foo", c.ColumnName; want != got {
		t.Errorf("want %s, but got %s", want, got)
	}

	if want, got := "STRING(MAX)", c.SpannerType; want != got {
		t.Errorf("want %s, but got %s", want, got)
	}
}

func TestApplyDMLFile(t *testing.T) {
	ctx := context.Background()

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	tests := map[string]struct {
		partitioned bool
	}{
		"normal DML":      {partitioned: false},
		"partitioned DML": {partitioned: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			key := "1"

			_, err := client.spannerClient.Apply(
				ctx,
				[]*spanner.Mutation{
					spanner.InsertOrUpdate(singerTable, []string{"SingerID", "FirstName"}, []interface{}{key, "Foo"}),
				},
			)
			if err != nil {
				t.Fatalf("failed to apply mutation: %v", err)
			}

			dml, err := ioutil.ReadFile("testdata/dml.sql")
			if err != nil {
				t.Fatalf("failed to read dml file: %v", err)
			}

			n, err := client.ApplyDMLFile(ctx, dml, test.partitioned)
			if err != nil {
				t.Fatalf("failed to apply dml file: %v", err)
			}

			if want, got := int64(1), n; want != got {
				t.Fatalf("want %d, but got %d", want, got)
			}

			row, err := client.spannerClient.Single().ReadRow(ctx, singerTable, spanner.Key{key}, []string{"FirstName"})
			if err != nil {
				t.Fatalf("failed to read row: %v", err)
			}

			s := &singer{}
			if err := row.ToStruct(s); err != nil {
				t.Fatalf("failed to convert row to struct: %v", err)
			}

			if want, got := "Bar", s.FirstName; want != got {
				t.Errorf("want %s, but got %s", want, got)
			}
		})
	}
}

func TestExecuteMigrations(t *testing.T) {
	ctx := context.Background()

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	// to ensure partitioned-dml (000003.sql) will be applied correctly, insert a row before migration.
	_, err := client.spannerClient.Apply(
		ctx,
		[]*spanner.Mutation{
			spanner.Insert(singerTable, []string{"SingerID", "FirstName"}, []interface{}{"1", "foo"}),
		},
	)
	if err != nil {
		t.Fatalf("failed to apply mutation: %v", err)
	}

	migrations, err := LoadMigrations("testdata/migrations", nil, false)
	if err != nil {
		t.Fatalf("failed to load migrations: %v", err)
	}

	var migrationsOutput MigrationsOutput
	// only apply 000002.sql by specifying limit 1.
	if migrationsOutput, err = client.ExecuteMigrations(ctx, migrations, 1, migrationTable); err != nil {
		t.Fatalf("failed to execute migration: %v", err)
	}

	if len(migrationsOutput) != 0 {
		t.Errorf("want zero length migrationInfo, but got %v", len(migrationsOutput))
	}

	// ensure that only 000002.sql has been applied.
	ensureMigrationColumn(t, ctx, client, "LastName", "STRING(MAX)", "YES")
	ensureMigrationVersionRecord(t, ctx, client, 2, false)
	ensureMigrationHistoryRecord(t, ctx, client, 2, false)

	if migrationsOutput, err = client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable); err != nil {
		t.Fatalf("failed to execute migration: %v", err)
	}

	if want, got := int64(1), migrationsOutput["000003.sql"].RowsAffected; want != got {
		t.Errorf("want %d, but got %d", want, got)
	}

	// ensure that 000003.sql and 000004.sql have been applied.
	ensureMigrationColumn(t, ctx, client, "LastName", "STRING(MAX)", "NO")
	ensureMigrationVersionRecord(t, ctx, client, 4, false)
	ensureMigrationHistoryRecord(t, ctx, client, 4, false)

	// ensure that schema is not changed and ExecuteMigrate is safely finished even though no migrations should be applied.
	ensureMigrationColumn(t, ctx, client, "LastName", "STRING(MAX)", "NO")
	ensureMigrationVersionRecord(t, ctx, client, 4, false)
}

func ensureMigrationColumn(t *testing.T, ctx context.Context, client *Client, columnName, spannerType, isNullable string) {
	t.Helper()

	ri := client.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT column_name, spanner_type, is_nullable FROM information_schema.columns WHERE table_catalog = '' AND table_name = @table AND column_name = @column",
		Params: map[string]interface{}{
			"table":  singerTable,
			"column": columnName,
		},
	})
	defer ri.Stop()

	row, err := ri.Next()
	if err == iterator.Done {
		t.Fatalf("failed to get table information: %v", err)
	}

	c := &column{}
	if err := row.ToStruct(c); err != nil {
		t.Fatalf("failed to convert row to struct: %v", err)
	}

	if want, got := spannerType, c.SpannerType; want != got {
		t.Errorf("want %s, but got %s", want, got)
	}

	if want, got := isNullable, c.IsNullable; want != got {
		t.Errorf("want %s, but got %s", want, got)
	}
}

func ensureMigrationHistoryRecord(t *testing.T, ctx context.Context, client *Client, version int64, dirty bool) {
	history, err := client.GetMigrationHistory(ctx, migrationTable)
	for i := range history {
		if history[i].Version == version && history[i].Dirty == dirty {
			return
		}
	}
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}

	t.Errorf("(version %d, dirty %v) not found in history", version, dirty)
}

func ensureMigrationVersionRecord(t *testing.T, ctx context.Context, client *Client, version int64, dirty bool) {
	t.Helper()

	row, err := client.spannerClient.Single().ReadRow(ctx, migrationTable, spanner.Key{version}, []string{"Version", "Dirty"})
	if err != nil {
		t.Fatalf("failed to read row: %v", err)
	}

	m := &migration{}
	if err := row.ToStruct(m); err != nil {
		t.Fatalf("failed to convert row to struct: %v", err)
	}

	if want, got := version, m.Version; want != got {
		t.Errorf("want %d, but got %d", want, got)
	}

	if want, got := dirty, m.Dirty; want != got {
		t.Errorf("want %t, but got %t", want, got)
	}
}

func TestGetSchemaMigrationVersion(t *testing.T) {
	ctx := context.Background()

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	version := 1
	dirty := false

	_, err := client.spannerClient.Apply(
		ctx,
		[]*spanner.Mutation{
			spanner.Insert(migrationTable, []string{"Version", "Dirty"}, []interface{}{version, dirty}),
		},
	)
	if err != nil {
		t.Fatalf("failed to apply mutation: %v", err)
	}

	v, d, err := client.GetSchemaMigrationVersion(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to get version: %v", err)
	}

	if want, got := uint(version), v; want != got {
		t.Errorf("want %d, but got %d", want, got)
	}

	if want, got := dirty, d; want != got {
		t.Errorf("want %t, but got %t", want, got)
	}
}

func TestSetSchemaMigrationVersion(t *testing.T) {
	ctx := context.Background()

	client, done := testClientWithDatabase(t, ctx)
	defer done()

	version := 1
	dirty := false

	_, err := client.spannerClient.Apply(
		ctx,
		[]*spanner.Mutation{
			spanner.Insert(migrationTable, []string{"Version", "Dirty"}, []interface{}{version, dirty}),
		},
	)
	if err != nil {
		t.Fatalf("failed to apply mutation: %v", err)
	}

	nextVersion := 2
	nextDirty := true

	if err := client.setSchemaMigrationVersion(ctx, uint(nextVersion), nextDirty, migrationTable); err != nil {
		t.Fatalf("failed to set version: %v", err)
	}

	ensureMigrationVersionRecord(t, ctx, client, int64(nextVersion), nextDirty)
}

func TestEnsureMigrationTable(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		table string
	}{
		"table already exists": {table: migrationTable},
		"table does not exist": {table: "SchemaMigrations2"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			client, done := testClientWithDatabase(t, ctx)
			defer done()

			if err := client.EnsureMigrationTable(ctx, test.table); err != nil {
				t.Fatalf("failed to ensure migration table: %v", err)
			}

			ri := client.spannerClient.Single().Query(ctx, spanner.Statement{
				SQL: "SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_name = @table",
				Params: map[string]interface{}{
					"table": test.table,
				},
			})
			defer ri.Stop()

			row, err := ri.Next()
			if err == iterator.Done {
				t.Fatalf("failed to get table information: %v", err)
			}

			ta := &table{}
			if err := row.ToStruct(ta); err != nil {
				t.Fatalf("failed to convert row to struct: %v", err)
			}

			if want, got := test.table, ta.TableName; want != got {
				t.Errorf("want %s, but got %s", want, got)
			}
		})
	}

	t.Run("also creates history table", func(t *testing.T) {
		client, done := testClientWithDatabase(t, ctx)
		defer done()

		if err := client.EnsureMigrationTable(ctx, migrationTable); err != nil {
			t.Fatalf("failed to ensure migration table: %v", err)
		}

		if client.tableExists(ctx, migrationTable+historyStr) == false {
			t.Fatal("failed to create history table")
		}
	})
}

func TestClient_DetermineUpgradeStatus(t *testing.T) {
	type args struct {
		tableName    string
		ddlStatement string
	}
	tests := []struct {
		name    string
		args    args
		want    UpgradeStatus
		wantErr bool
	}{
		{
			string(FirstRun),
			args{"NonExistentTable", ""},
			FirstRun,
			false,
		},
		{
			string(ExistingMigrationsNoUpgrade),
			args{migrationTable, "DROP TABLE " + migrationTable + historyStr},
			ExistingMigrationsNoUpgrade,
			false,
		},
		{
			string(ExistingMigrationsUpgradeStarted),
			args{migrationTable, createUpgradeIndicatorSql},
			ExistingMigrationsUpgradeStarted,
			false,
		},
		{
			string(ExistingMigrationsUpgradeCompleted),
			args{migrationTable, ""},
			ExistingMigrationsUpgradeCompleted,
			false,
		},
		{
			"UndeterminedState",
			args{"NonExistentTable", createUpgradeIndicatorSql},
			"",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client, done := testClientWithDatabase(t, ctx)
			defer done()

			if tt.args.ddlStatement != "" {
				err := client.ApplyDDL(ctx, []string{tt.args.ddlStatement})
				if err != nil {
					t.Error(err)
				}
			}

			got, err := client.DetermineUpgradeStatus(ctx, tt.args.tableName)

			if (err != nil) != tt.wantErr {
				t.Errorf("DetermineUpgradeStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DetermineUpgradeStatus() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHotfixMigration(t *testing.T) {
	ctx := context.Background()
	client, done := testClientWithDatabase(t, ctx)
	defer done()

	// apply changes from "trunk": [100, 200]
	migrations, err := LoadMigrations("testdata/hotfix/a", nil, false)
	if err != nil {
		t.Fatalf("failed to load migrations: %v", err)
	}
	if _, err = client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable); err != nil {
		t.Fatalf("failed to execute migration: %v", err)
	}
	history, err := client.GetMigrationHistory(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to get migration history: %v", err)
	}
	if len(history) != 2 {
		t.Errorf("incorrect history versions: %+v", history)
	}
	ensureMigrationHistoryRecord(t, ctx, client, 100, false)
	ensureMigrationHistoryRecord(t, ctx, client, 200, false)

	// apply changes from "hotfix" branch: [101]
	migrations, err = LoadMigrations("testdata/hotfix/b", nil, false)
	if err != nil {
		t.Fatalf("failed to load migrations: %v", err)
	}
	if _, err := client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable); err != nil {
		t.Fatalf("failed to execute migration: %v", err)
	}
	history, err = client.GetMigrationHistory(ctx, migrationTable)
	if err != nil {
		t.Fatalf("failed to get migration history: %v", err)
	}
	if len(history) != 3 {
		t.Errorf("incorrect history versions: %+v", history)
	}
	ensureMigrationHistoryRecord(t, ctx, client, 101, false)
}

func TestUpgrade(t *testing.T) {
	t.Run("PriorMigrationsBackfilledInHistoryTable", func(t *testing.T) {
		ctx := context.Background()
		client, done := testClientWithDatabase(t, ctx)
		defer done()

		// run migrations
		migrations, err := LoadMigrations("testdata/migrations", nil, false)
		if err != nil {
			t.Fatalf("failed to load migrations: %v", err)
		}
		if _, err := client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable); err != nil {
			t.Fatalf("failed to execute migration: %v", err)
		}
		expected, err := client.GetMigrationHistory(ctx, migrationTable)
		if err != nil {
			t.Fatalf("failed to get migration history: %v", err)
		}

		// clear history table
		if err := client.ApplyDDL(ctx, []string{"DROP TABLE " + migrationTable + historyStr}); err != nil {
			t.Fatalf("failed to drop migration history: %v", err)
		}
		if err := client.EnsureMigrationTable(ctx, migrationTable); err != nil {
			t.Fatalf("failed to recreate migration table: %v", err)
		}
		if client.tableExists(ctx, upgradeIndicator) == false {
			t.Error("upgrade indicator should exist")
		}
		if _, err := client.UpgradeExecuteMigrations(ctx, migrations, len(migrations), migrationTable); err != nil {
			t.Fatalf("failed to execute migration: %v", err)
		}

		// history is backfilled
		actual, err := client.GetMigrationHistory(ctx, migrationTable)
		if err != nil {
			t.Fatalf("failed to get migration history: %v", err)
		}
		if len(expected) != len(actual) {
			t.Error("missing versions in backfilled history")
		}
		if client.tableExists(ctx, upgradeIndicator) == true {
			t.Error("upgrade indicator should be dropped")
		}

		contains := func(m []MigrationHistoryRecord, v int64) bool {
			for i := range m {
				if m[i].Version == v {
					return true
				}
			}
			return false
		}

		if (contains(actual, 2) && contains(actual, 3) && contains(actual, 4)) == false {
			t.Errorf("missing version in history table %+v", actual)
		}
	})
}

func testClientWithDatabase(t *testing.T, ctx context.Context) (*Client, func()) {
	t.Helper()

	project := os.Getenv(envSpannerProjectID)
	if project == "" {
		t.Fatalf("must set %s", envSpannerProjectID)
	}

	instance := os.Getenv(envSpannerInstanceID)
	if instance == "" {
		t.Fatalf("must set %s", envSpannerInstanceID)
	}

	// TODO: take random database name and run tests parallelly.
	database := os.Getenv(envSpannerDatabaseID)
	if database == "" {
		id := uuid.New()
		database = fmt.Sprintf("wrench-test-%s", id.String()[:8])
		t.Log("creating " + database)
	}

	config := &Config{
		Project:  project,
		Instance: instance,
		Database: database,
	}

	client, err := NewClient(ctx, config)
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	ddl, err := ioutil.ReadFile("testdata/schema.sql")
	if err != nil {
		t.Fatalf("failed to read schema file: %v", err)
	}

	if err := client.CreateDatabase(ctx, ddl); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// emulator weirdness in CI
	time.Sleep(100 * time.Millisecond)

	return client, func() {
		defer client.Close()

		if err := client.DropDatabase(ctx); err != nil {
			t.Fatalf("failed to delete database: %v", err)
		}
		t.Log("dropped database " + database)
	}
}

func Test_parseDDL(t *testing.T) {
	type args struct {
		statement string
	}
	tests := []struct {
		name    string
		args    args
		wantDdl SchemaDDL
		wantErr bool
	}{
		{
			name: "CREATE TABLE",
			args: args{"CREATE TABLE Example(ID string) PRIMARY KEY(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE TABLE Example(ID string) PRIMARY KEY(ID)",
				Filename:   "example.sql",
				ObjectType: "table",
			},
		},
		{
			name: "CREATE INDEX",
			args: args{"CREATE INDEX IX_Example ON Example(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE INDEX IX_Example ON Example(ID)",
				Filename:   "ix_example.sql",
				ObjectType: "index",
			},
		},
		{
			name: "CREATE NULL_FILTERED INDEX",
			args: args{"CREATE NULL_FILTERED INDEX NFX_Example ON Example(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE NULL_FILTERED INDEX NFX_Example ON Example(ID)",
				Filename:   "nfx_example.sql",
				ObjectType: "index",
			},
		},
		{
			name: "CREATE UNIQUE INDEX",
			args: args{"CREATE UNIQUE INDEX UX_Example ON Example(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE UNIQUE INDEX UX_Example ON Example(ID)",
				Filename:   "ux_example.sql",
				ObjectType: "index",
			},
		},
		{
			name: "CREATE UNIQUE NULL_FILTERED INDEX",
			args: args{"CREATE UNIQUE NULL_FILTERED INDEX UX_Example ON Example(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE UNIQUE NULL_FILTERED INDEX UX_Example ON Example(ID)",
				Filename:   "ux_example.sql",
				ObjectType: "index",
			},
		},
		{
			name: "CREATE		UNIQUE  NULL_FILTERED   INDEX",
			args: args{"CREATE\t\tUNIQUE  NULL_FILTERED   INDEX UX_Example ON Example(ID)"},
			wantDdl: SchemaDDL{
				Statement:  "CREATE\t\tUNIQUE  NULL_FILTERED   INDEX UX_Example ON Example(ID)",
				Filename:   "ux_example.sql",
				ObjectType: "index",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDdl, err := parseDDL(tt.args.statement)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDDL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDdl, tt.wantDdl) {
				t.Errorf("parseDDL() gotDdl = %v, want %v", gotDdl, tt.wantDdl)
			}
		})
	}
}

func TestClient_RepairMigration(t *testing.T) {
	ctx := context.Background()
	client, done := testClientWithDatabase(t, ctx)
	defer done()

	// add LastName NULLABLE
	err := migrateUpDir(t, ctx, client, "testdata/migrations", 3, 4)
	require.NoError(t, err, "error running migrations")

	// add row with NULL LastName
	_, err = client.spannerClient.Apply(ctx, []*spanner.Mutation{spanner.Insert(singerTable, []string{"SingerID", "FirstName"}, []any{"ABC", "Fred"})})
	require.NoError(t, err, "failed to insert row")

	// make dirty with bad migration
	err = migrateUpDir(t, ctx, client, "testdata/migrations", 3)
	assert.EqualError(t, err, "Cannot specify a null value for column: LastName in table: Singers referenced by key: {String(\"ABC\")}")

	assertDirtyCount := func(isDirty bool, expected int64) {
		dirtyCount, err := spannerz.ReadColumnSQL[int64](ctx, client.spannerClient.Single(),
			fmt.Sprintf("select count(1) from SchemaMigrationsHistory where Dirty = %v", isDirty))
		assert.NoError(t, err)
		assert.EqualValues(t, expected, dirtyCount)
	}
	const dirty, clean = true, false
	assertDirtyCount(dirty, 1)
	assertDirtyCount(clean, 1)
	version, isDirty, err := client.GetSchemaMigrationVersion(ctx, migrationTable)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, version) // failed on 4
	assert.True(t, isDirty)

	// is idempotent
	for i := 0; i < 2; i++ {
		// repair migration
		err = client.RepairMigration(ctx, migrationTable)
		assert.NoError(t, err)

		assertDirtyCount(dirty, 0)
		assertDirtyCount(clean, 1)
		version, isDirty, err = client.GetSchemaMigrationVersion(ctx, migrationTable)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, version) // back to 2 since 3 was skipped
		assert.False(t, isDirty)
	}
}

func Test_MigrationInfoString(t *testing.T) {
	tests := []struct {
		testName        string
		migrationInfo   MigrationsOutput
		exptectedOutput string
	}{
		{
			testName:        "no results",
			migrationInfo:   MigrationsOutput{},
			exptectedOutput: "",
		},
		{
			testName:        "unitiated results - panic resiliant",
			exptectedOutput: "",
		},
		{
			testName: "one result",
			migrationInfo: MigrationsOutput{
				"i-deleted-everything.sql": migrationInfo{
					RowsAffected: 2000,
				},
			},
			exptectedOutput: "Migration Information:\ni-deleted-everything.sql - rows affected: 2000\n",
		},
		{
			testName: "many results",
			migrationInfo: MigrationsOutput{
				"0001-i-am-a-cool-update.sql": migrationInfo{
					RowsAffected: 20,
				},
				"0002-not-as-cool-as-me.sql": migrationInfo{
					RowsAffected: 25,
				},
				"0003-i-deleted-everything.sql": migrationInfo{
					RowsAffected: 2000,
				},
			},
			exptectedOutput: "Migration Information:\n0001-i-am-a-cool-update.sql - rows affected: 20\n0002-not-as-cool-as-me.sql - rows affected: 25\n0003-i-deleted-everything.sql - rows affected: 2000\n",
		},
	}

	for _, test := range tests {
		output := test.migrationInfo.String()
		assert.Equal(t, test.exptectedOutput, output)
	}
}

func migrateUpDir(t *testing.T, ctx context.Context, client *Client, dir string, toSkip ...uint) error {
	t.Helper()
	migrations, err := LoadMigrations(dir, toSkip, false)
	if err != nil {
		return err
	}

	_, err = client.ExecuteMigrations(ctx, migrations, len(migrations), migrationTable)
	if err != nil {
		return err
	}

	return nil
}
