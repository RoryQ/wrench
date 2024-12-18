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
	"cmp"
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/sourcegraph/conc/pool"

	"github.com/roryq/wrench/pkg/spannerz"
	"github.com/roryq/wrench/pkg/xregexp"

	"google.golang.org/grpc/codes"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	vkit "cloud.google.com/go/spanner/apiv1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/roryq/wrench/pkg/spanner/dataloader"
)

const (
	ddlStatementsSeparator             = ";"
	upgradeIndicator                   = "wrench_upgrade_indicator"
	historyStr                         = "History"
	lockStr                            = "Lock"
	FirstRun                           = UpgradeStatus("FirstRun")
	ExistingMigrationsNoUpgrade        = UpgradeStatus("NoUpgrade")
	ExistingMigrationsUpgradeStarted   = UpgradeStatus("Started")
	ExistingMigrationsUpgradeCompleted = UpgradeStatus("Completed")
	createUpgradeIndicatorFormatString = `CREATE TABLE %s (Dummy INT64 NOT NULL) PRIMARY KEY(Dummy)`
)

var (
	createUpgradeIndicatorSql = fmt.Sprintf(createUpgradeIndicatorFormatString, upgradeIndicator)
	indexOptions              = `unique\s+|null_filtered\s+|unique\s+null_filtered\s+`
	ddlParse                  = regexp.MustCompile(`(?i)create\s+(?P<ObjectType>(table|(` + indexOptions + `)?index|view))\s+(?P<ObjectName>\w+).*`)
)

type UpgradeStatus string

type table struct {
	TableName string `spanner:"table_name"`
}

type Client struct {
	config             *Config
	spannerClient      *spanner.Client
	spannerAdminClient *admin.DatabaseAdminClient
}

type MigrationHistoryRecord struct {
	Version  int64     `spanner:"Version"`
	Dirty    bool      `spanner:"Dirty"`
	Created  time.Time `spanner:"Created"`
	Modified time.Time `spanner:"Modified"`
}

func NewClient(ctx context.Context, config *Config) (*Client, error) {
	opts := make([]option.ClientOption, 0)
	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	callOptions := &vkit.CallOptions{}
	if config.StmtTimeout > 0 {
		callOptions.ExecuteSql = []gax.CallOption{gax.WithTimeout(config.StmtTimeout)}
	}
	spannerClient, err := spanner.NewClientWithConfig(ctx, config.URL(), spanner.ClientConfig{
		SessionPoolConfig:    spanner.DefaultSessionPoolConfig,
		DisableRouteToLeader: false,
		CallOptions:          callOptions,
	}, opts...)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	spannerAdminClient, err := admin.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		spannerClient.Close()
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	return &Client{
		config:             config,
		spannerClient:      spannerClient,
		spannerAdminClient: spannerAdminClient,
	}, nil
}

func (c *Client) CreateDatabase(ctx context.Context, ddl []byte) error {
	statements, err := toStatements(ddl)
	if err != nil {
		return &Error{
			Code: ErrorCodeCreateDatabase,
			err:  err,
		}
	}

	createReq := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", c.config.Project, c.config.Instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", c.config.Database),
		ExtraStatements: statements,
	}

	op, err := c.spannerAdminClient.CreateDatabase(ctx, createReq)
	if err != nil {
		return &Error{
			Code: ErrorCodeCreateDatabase,
			err:  err,
		}
	}

	_, err = op.Wait(ctx)
	if err != nil {
		return &Error{
			Code: ErrorCodeWaitOperation,
			err:  err,
		}
	}

	return nil
}

func (c *Client) DropDatabase(ctx context.Context) error {
	req := &databasepb.DropDatabaseRequest{Database: c.config.URL()}

	if err := c.spannerAdminClient.DropDatabase(ctx, req); err != nil {
		return &Error{
			Code: ErrorCodeDropDatabase,
			err:  err,
		}
	}

	return nil
}

func (c *Client) TruncateAllTables(ctx context.Context) error {
	var m []*spanner.Mutation

	ri := c.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_schema = ''",
	})
	defer ri.Stop()
	err := ri.Do(func(row *spanner.Row) error {
		t := &table{}
		if err := row.ToStruct(t); err != nil {
			return err
		}

		if t.TableName == "SchemaMigrations" {
			return nil
		}

		m = append(m, spanner.Delete(t.TableName, spanner.AllKeys()))
		return nil
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	_, err = c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(m)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeTruncateAllTables,
			err:  err,
		}
	}

	return nil
}

type SchemaDDL struct {
	Statement  string
	Filename   string
	ObjectType string
}

func parseDDL(statement string) (ddl SchemaDDL, err error) {
	matches, found := xregexp.FindMatchGroups(ddlParse, statement)
	if !found {
		return ddl, errors.New("could not parse DDL statement")
	}

	objectType := strings.ToLower(matches["ObjectType"])
	// put all indexes in the same group
	if strings.HasSuffix(objectType, "index") {
		objectType = "index"
	}

	ddl = SchemaDDL{
		Statement:  statement,
		ObjectType: objectType,
		Filename:   fmt.Sprintf("%s.sql", strings.ToLower(matches["ObjectName"])),
	}

	if ddl.ObjectType == "" {
		return ddl, errors.New("could not determine the object type")
	}

	return ddl, nil
}

func (c *Client) LoadDDLs(ctx context.Context) ([]SchemaDDL, error) {
	req := &databasepb.GetDatabaseDdlRequest{Database: c.config.URL()}

	res, err := c.spannerAdminClient.GetDatabaseDdl(ctx, req)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeLoadSchema,
			err:  err,
		}
	}

	ddls := make([]SchemaDDL, 0)
	for i := range res.Statements {
		ddl, err := parseDDL(res.Statements[i])
		if err != nil {
			return nil, &Error{
				Code: ErrorCodeLoadSchema,
				err:  err,
			}
		}
		ddls = append(ddls, ddl)
	}

	return ddls, nil
}

func (c *Client) LoadDDL(ctx context.Context) ([]byte, error) {
	req := &databasepb.GetDatabaseDdlRequest{Database: c.config.URL()}

	res, err := c.spannerAdminClient.GetDatabaseDdl(ctx, req)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeLoadSchema,
			err:  err,
		}
	}

	var schema []byte
	last := len(res.Statements) - 1
	for index, statement := range res.Statements {
		if index != last {
			statement += ddlStatementsSeparator + "\n\n"
		} else {
			statement += ddlStatementsSeparator + "\n"
		}

		schema = append(schema[:], []byte(statement)[:]...)
	}

	return schema, nil
}

type StaticData struct {
	TableName  string
	Statements []string
	Count      int
}

func (s StaticData) ToFileName() string {
	return strings.ToLower(s.TableName) + ".sql"
}

func (c *Client) LoadStaticDatas(ctx context.Context, tables []string, customSort map[string]string) ([]StaticData, error) {
	datas := make([]StaticData, 0, len(tables))
	for _, t := range tables {
		d, err := c.loadStaticData(ctx, t, customSort[t])
		if err != nil {
			return nil, err
		}
		datas = append(datas, d)
	}

	return datas, nil
}

func (c *Client) loadStaticData(ctx context.Context, table string, customSort string) (StaticData, error) {
	data := StaticData{
		TableName: table,
	}
	query, err := c.staticDataQuery(ctx, table, customSort)
	if err != nil {
		return StaticData{}, err
	}

	err = c.spannerClient.
		Single().
		Query(ctx, query).
		Do(func(r *spanner.Row) error {
			insert, err := dataloader.RowToInsertStatement(table, r)
			if err != nil {
				return err
			}
			data.Statements = append(data.Statements, insert)
			data.Count++
			return nil
		})
	if err != nil {
		return StaticData{}, err
	}

	return data, nil
}

func (c *Client) staticDataQuery(ctx context.Context, table, customOrderBy string) (spanner.Statement, error) {
	var orderByClause string

	// use primary key sort by default
	if customOrderBy == "" {
		var columnOrders []string
		stmt := spanner.NewStatement(
			"SELECT COLUMN_NAME, COLUMN_ORDERING FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
				"WHERE INDEX_NAME='PRIMARY_KEY' AND TABLE_NAME=@tableName " +
				"ORDER BY ORDINAL_POSITION")
		stmt.Params["tableName"] = table
		err := c.spannerClient.
			Single().
			Query(ctx, stmt).
			Do(func(r *spanner.Row) error {
				var name, order string
				if err := r.Columns(&name, &order); err != nil {
					return err
				}

				columnOrders = append(columnOrders, fmt.Sprintf("%s %s", name, order))

				return nil
			})
		if err != nil {
			return spanner.Statement{}, err
		}

		if len(columnOrders) > 0 {
			orderByClause = "\nORDER BY " + strings.Join(columnOrders, ", ")
		}
	} else {
		orderByClause = "\nORDER BY " + customOrderBy
	}

	return spanner.NewStatement("SELECT * FROM " + table + orderByClause), nil
}

func (c *Client) ApplyDDLFile(ctx context.Context, ddl []byte) error {
	statements, err := toStatements(ddl)
	if err != nil {
		return err
	}
	return c.ApplyDDL(ctx, statements)
}

func (c *Client) ApplyDDL(ctx context.Context, statements []string) error {
	req := &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.config.URL(),
		Statements: statements,
	}

	op, err := c.spannerAdminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return &Error{
			Code: ErrorCodeUpdateDDL,
			err:  err,
		}
	}

	err = op.Wait(ctx)
	if err != nil {
		return &Error{
			Code: ErrorCodeWaitOperation,
			err:  err,
		}
	}

	return nil
}

func (c *Client) ApplyDMLFile(ctx context.Context, dml []byte, partitioned bool, concurrency int) (int64, error) {
	statements, err := toStatements(dml)
	if err != nil {
		return 0, err
	}

	if partitioned {
		return c.ApplyPartitionedDML(ctx, statements, concurrency)
	}
	return c.ApplyDML(ctx, statements)
}

func (c *Client) ApplyDML(ctx context.Context, statements []string) (int64, error) {
	numAffectedRows := int64(0)
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		for _, s := range statements {
			num, err := tx.Update(ctx, spanner.Statement{
				SQL: s,
			})
			if err != nil {
				return err
			}
			numAffectedRows += num
		}
		return nil
	})
	if err != nil {
		return 0, &Error{
			Code: ErrorCodeUpdateDML,
			err:  err,
		}
	}

	return numAffectedRows, nil
}

func (c *Client) ApplyPartitionedDML(ctx context.Context, statements []string, concurrency int) (int64, error) {
	numAffectedRows := atomic.Int64{}

	concurrency = cmp.Or(concurrency, 1)
	p := pool.New().WithMaxGoroutines(concurrency).WithErrors()
	for _, s := range statements {
		p.Go(func() error {
			num, err := c.spannerClient.PartitionedUpdate(ctx, spanner.Statement{
				SQL: s,
			})
			if err != nil {
				return err
			}

			numAffectedRows.Add(num)
			return nil
		})
	}

	err := p.Wait()
	if err != nil {
		return numAffectedRows.Load(), &Error{
			Code: ErrorCodeUpdatePartitionedDML,
			err:  err,
		}
	}

	return numAffectedRows.Load(), nil
}

func (c *Client) UpgradeExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string) (MigrationsOutput, error) {
	err := c.backfillMigrations(ctx, migrations, tableName)
	if err != nil {
		return nil, err
	}

	migrationsOutput, err := c.ExecuteMigrations(ctx, migrations, limit, tableName, 1)
	if err != nil {
		return nil, err
	}

	err = c.markUpgradeComplete(ctx)
	if err != nil {
		return nil, err
	}

	return migrationsOutput, nil
}

func (c *Client) backfillMigrations(ctx context.Context, migrations Migrations, tableName string) error {
	v, d, err := c.GetSchemaMigrationVersion(ctx, tableName)
	if err != nil {
		return err
	}

	historyTableName := tableName + historyStr
	_, err = c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, rw *spanner.ReadWriteTransaction) error {
		for i := range migrations {
			if v > migrations[i].Version {
				if err := c.upsertVersionHistory(ctx, rw, int64(migrations[i].Version), false, historyTableName); err != nil {
					return err
				}
			} else if v == migrations[i].Version {
				if err := c.upsertVersionHistory(ctx, rw, int64(migrations[i].Version), d, historyTableName); err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) upsertVersionHistory(ctx context.Context, rw *spanner.ReadWriteTransaction, version int64, dirty bool, historyTableName string) error {
	_, err := rw.ReadRow(ctx, historyTableName, spanner.Key{version}, []string{"Version", "Dirty", "Created", "Modified"})
	if err != nil {
		// insert
		if spanner.ErrCode(err) == codes.NotFound {
			return rw.BufferWrite([]*spanner.Mutation{
				spanner.Insert(historyTableName,
					[]string{"Version", "Dirty", "Created", "Modified"},
					[]interface{}{version, dirty, spanner.CommitTimestamp, spanner.CommitTimestamp}),
			})
		}
		return err
	}

	// update
	return rw.BufferWrite([]*spanner.Mutation{
		spanner.Update(historyTableName,
			[]string{"Version", "Dirty", "Modified"},
			[]interface{}{version, dirty, spanner.CommitTimestamp}),
	})
}

func (c *Client) markUpgradeComplete(ctx context.Context) error {
	err := c.ApplyDDL(ctx, []string{"DROP TABLE " + upgradeIndicator})
	if err != nil {
		return &Error{
			Code: ErrorCodeCompleteUpgrade,
			err:  err,
		}
	}

	return nil
}

func (c *Client) GetMigrationHistory(ctx context.Context, versionTableName string) ([]MigrationHistoryRecord, error) {
	if !c.tableExists(ctx, versionTableName) {
		return nil, &Error{
			Code: ErrorCodeGetMigrationVersion,
			err:  errors.New("Migration history table not found. Run a migration to enable history"),
		}
	}

	history := make([]MigrationHistoryRecord, 0)
	stmt := spanner.NewStatement("SELECT Version, Dirty, Created, Modified FROM " + versionTableName + historyStr)
	err := c.spannerClient.Single().Query(ctx, stmt).Do(func(r *spanner.Row) error {
		version := MigrationHistoryRecord{}
		if err := r.ToStruct(&version); err != nil {
			return err
		}
		history = append(history, version)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return history, nil
}

type MigrationsOutput map[string]migrationInfo

type migrationInfo struct {
	RowsAffected int64
}

func (i MigrationsOutput) String() string {
	if len(i) == 0 {
		return ""
	}

	var filenames []string
	for filename := range i {
		filenames = append(filenames, filename)
	}

	sort.StringSlice(filenames).Sort()

	output := "Migration Information:"
	for _, filename := range filenames {
		migrationInfo := i[filename]
		output = fmt.Sprintf("%s\n%s - rows affected: %d", output, filename, migrationInfo.RowsAffected)
	}

	return fmt.Sprintf("%s\n", output)
}

func (c *Client) ExecuteMigrations(ctx context.Context, migrations Migrations, limit int, tableName string, partitionedConcurrency int) (MigrationsOutput, error) {
	sort.Sort(migrations)

	version, dirty, err := c.GetSchemaMigrationVersion(ctx, tableName)
	if err != nil {
		var se *Error
		if !errors.As(err, &se) || se.Code != ErrorCodeNoMigration {
			return nil, &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}
	}

	if dirty {
		return nil, &Error{
			Code: ErrorCodeMigrationVersionDirty,
			err:  fmt.Errorf("database version: %d is dirty, please fix it.", version),
		}
	}

	history, err := c.GetMigrationHistory(ctx, tableName)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeExecuteMigrations,
			err:  err,
		}
	}
	applied := make(map[int64]bool)
	for i := range history {
		applied[history[i].Version] = true
	}

	var migrationsOutput MigrationsOutput = make(MigrationsOutput)
	var count int
	for _, m := range migrations {
		if limit == 0 {
			break
		}

		if applied[int64(m.Version)] {
			continue
		}

		if err := c.setSchemaMigrationVersion(ctx, m.Version, true, tableName); err != nil {
			return nil, &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}

		switch m.Kind {
		case StatementKindDDL:
			if err := c.ApplyDDL(ctx, m.Statements); err != nil {
				return nil, &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}
		case StatementKindDML:
			rowsAffected, err := c.ApplyDML(ctx, m.Statements)
			if err != nil {
				return nil, &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}

			migrationsOutput[m.FileName] = migrationInfo{
				RowsAffected: rowsAffected,
			}
		case StatementKindPartitionedDML:
			rowsAffected, err := c.ApplyPartitionedDML(ctx, m.Statements, partitionedConcurrency)
			if err != nil {
				return nil, &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  err,
				}
			}

			migrationsOutput[m.FileName] = migrationInfo{
				RowsAffected: rowsAffected,
			}
		default:
			return nil, &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  fmt.Errorf("Unknown query type, version: %d", m.Version),
			}
		}

		if m.Name != "" {
			fmt.Printf("%d/up %s\n", m.Version, m.Name)
		} else {
			fmt.Printf("%d/up\n", m.Version)
		}

		if err := c.setSchemaMigrationVersion(ctx, m.Version, false, tableName); err != nil {
			return nil, &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  err,
			}
		}

		count++
		if limit > 0 && count == limit {
			break
		}
	}

	if count == 0 {
		fmt.Println("no change")
	}

	return migrationsOutput, nil
}

func (c *Client) GetSchemaMigrationVersion(ctx context.Context, tableName string) (uint, bool, error) {
	stmt := spanner.Statement{
		SQL: `SELECT Version, Dirty FROM ` + tableName + ` LIMIT 1`,
	}
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return 0, false, &Error{
				Code: ErrorCodeNoMigration,
				err:  errors.New("No migration."),
			}
		}
		return 0, false, &Error{
			Code: ErrorCodeGetMigrationVersion,
			err:  err,
		}
	}

	var (
		v     int64
		dirty bool
	)
	if err := row.Columns(&v, &dirty); err != nil {
		return 0, false, &Error{
			Code: ErrorCodeGetMigrationVersion,
			err:  err,
		}
	}

	return uint(v), dirty, nil
}

// setSchemaMigrationVersion will set a specific version in the version and history table without checking existing state
func (c *Client) setSchemaMigrationVersion(ctx context.Context, version uint, dirty bool, tableName string) error {
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		m := setSchemaVersionMutations(tableName, version, dirty)
		if err := tx.BufferWrite(m); err != nil {
			return err
		}

		return c.upsertVersionHistory(ctx, tx, int64(version), dirty, tableName+historyStr)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeSetMigrationVersion,
			err:  err,
		}
	}

	return nil
}

func setSchemaVersionMutations(tableName string, version uint, dirty bool) []*spanner.Mutation {
	m := []*spanner.Mutation{
		spanner.Delete(tableName, spanner.AllKeys()),
		spanner.Insert(
			tableName,
			[]string{"Version", "Dirty"},
			[]interface{}{int64(version), dirty},
		),
	}
	return m
}

// RepairMigration will delete the dirty rows in the version and history tables
func (c *Client) RepairMigration(ctx context.Context, tableName string) error {
	tableNameHistory := tableName + historyStr

	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		m, err := deleteDirtyHistory(ctx, tx, tableNameHistory)
		if err != nil {
			return err
		}

		version, err := resetSchemaVersion(ctx, tx, tableNameHistory, tableName)
		if err != nil {
			return err
		}
		m = append(m, version...)

		return tx.BufferWrite(m)
	})
	if err != nil {
		return &Error{
			Code: ErrorCodeUndirtyMigration,
			err:  err,
		}
	}

	return nil
}

func resetSchemaVersion(ctx context.Context, tx *spanner.ReadWriteTransaction, tableNameHistory string, tableName string) ([]*spanner.Mutation, error) {
	latestSQL := "select * from " + tableNameHistory + " where dirty = FALSE order by version limit 1"
	latest, err := spannerz.GetSQL[MigrationHistoryRecord](ctx, tx, latestSQL)
	if err != nil {
		return nil, err
	}
	if len(latest) != 1 {
		return nil, errors.New("no undirty versions found")
	}
	version := setSchemaVersionMutations(tableName, uint(latest[0].Version), false)
	return version, nil
}

func deleteDirtyHistory(ctx context.Context, tx *spanner.ReadWriteTransaction, tableNameHistory string) ([]*spanner.Mutation, error) {
	sql := "select * from " + tableNameHistory + " where dirty = TRUE"
	dirty, err := spannerz.GetSQL[MigrationHistoryRecord](ctx, tx, sql)
	if err != nil {
		return nil, err
	}

	keys := []spanner.Key{}
	for _, record := range dirty {
		keys = append(keys, spanner.Key{record.Version})
	}

	m := []*spanner.Mutation{spanner.Delete(tableNameHistory, spanner.KeySetFromKeys(keys...))}
	return m, nil
}

func (c *Client) Close() error {
	c.spannerClient.Close()
	if err := c.spannerAdminClient.Close(); err != nil {
		return &Error{
			err:  err,
			Code: ErrorCodeCloseClient,
		}
	}

	return nil
}

func (c *Client) EnsureMigrationTable(ctx context.Context, tableName string) error {
	fmtErr := func(err error) *Error {
		return &Error{
			Code: ErrorCodeEnsureMigrationTables,
			err:  err,
		}
	}
	status, err := c.DetermineUpgradeStatus(ctx, tableName)
	if err != nil {
		return fmtErr(err)
	}

	switch status {
	case FirstRun:
		if err := c.createVersionTable(ctx, tableName); err != nil {
			return fmtErr(err)
		}
		if err := c.createHistoryTable(ctx, tableName+historyStr); err != nil {
			return fmtErr(err)
		}
		if err := c.SetupMigrationLock(ctx, tableName+lockStr); err != nil {
			return fmtErr(err)
		}
	case ExistingMigrationsNoUpgrade:
		if err := c.createUpgradeIndicatorTable(ctx); err != nil {
			return fmtErr(err)
		}
		if err := c.createHistoryTable(ctx, tableName+historyStr); err != nil {
			return fmtErr(err)
		}
		if err := c.SetupMigrationLock(ctx, tableName+lockStr); err != nil {
			return fmtErr(err)
		}
	}

	return nil
}

func (c *Client) DetermineUpgradeStatus(ctx context.Context, tableName string) (UpgradeStatus, error) {
	stmt := spanner.NewStatement(`SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_schema = ''
AND table_name in (@version, @history, @indicator)`)
	stmt.Params["version"] = tableName
	stmt.Params["history"] = tableName + historyStr
	stmt.Params["indicator"] = upgradeIndicator
	iter := c.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	tables := make(map[string]bool)
	err := iter.Do(func(r *spanner.Row) error {
		t := &table{}
		if err := r.ToStruct(t); err != nil {
			return err
		}
		tables[t.TableName] = true
		return nil
	})
	if err != nil {
		return "", err
	}

	switch {
	case len(tables) == 0:
		return FirstRun, nil
	case len(tables) == 1 && tables[tableName]:
		return ExistingMigrationsNoUpgrade, nil
	case len(tables) == 2 && tables[tableName] && tables[tableName+historyStr]:
		return ExistingMigrationsUpgradeCompleted, nil
	case len(tables) > 1 && tables[tableName] && tables[upgradeIndicator]:
		return ExistingMigrationsUpgradeStarted, nil
	default:
		return "", fmt.Errorf("undetermined state of schema version tables %+v", tables)
	}
}

func (c *Client) tableExists(ctx context.Context, tableName string) bool {
	ri := c.spannerClient.Single().Query(ctx, spanner.Statement{
		SQL:    "SELECT table_name FROM information_schema.tables WHERE table_catalog = '' AND table_name = @table",
		Params: map[string]interface{}{"table": tableName},
	})
	defer ri.Stop()
	_, err := ri.Next()
	return err != iterator.Done
}

func (c *Client) createHistoryTable(ctx context.Context, historyTableName string) error {
	if c.tableExists(ctx, historyTableName) {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
	Dirty BOOL NOT NULL,
	Created TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	Modified TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY(Version)`, historyTableName)

	return c.ApplyDDL(ctx, []string{stmt})
}

func (c *Client) createUpgradeIndicatorTable(ctx context.Context) error {
	if c.tableExists(ctx, upgradeIndicator) {
		return nil
	}

	stmt := fmt.Sprintf(createUpgradeIndicatorFormatString, upgradeIndicator)

	return c.ApplyDDL(ctx, []string{stmt})
}

func (c *Client) createVersionTable(ctx context.Context, tableName string) error {
	if c.tableExists(ctx, tableName) {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE TABLE %s (
    Version INT64 NOT NULL,
    Dirty    BOOL NOT NULL
	) PRIMARY KEY(Version)`, tableName)

	return c.ApplyDDL(ctx, []string{stmt})
}

type MigrationLock struct {
	Success        bool
	Release        func()
	LockIdentifier string    `spanner:"LockIdentifier"`
	Expiry         time.Time `spanner:"Expiry"`
}

func (c *Client) SetupMigrationLock(ctx context.Context, tableName string) error {
	if !c.tableExists(ctx, tableName) {
		sql := fmt.Sprintf("CREATE TABLE %s(ID INT64, LockIdentifier STRING(200), Expiry TIMESTAMP) PRIMARY KEY(ID)", tableName)
		err := c.ApplyDDL(ctx, []string{sql})
		if err != nil {
			return err
		}
	}

	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, trx *spanner.ReadWriteTransaction) error {
		row, err := trx.ReadRow(ctx, tableName, spanner.Key{spanner.NullInt64{}}, []string{"LockIdentifier", "Expiry"})
		if err != nil {
			// insert
			if spanner.ErrCode(err) == codes.NotFound {
				return trx.BufferWrite([]*spanner.Mutation{
					spanner.Insert(tableName,
						[]string{"ID"},
						[]interface{}{spanner.NullInt64{}}),
				})
			}
			return err
		}

		lock := MigrationLock{}
		err = row.ToStruct(&lock)
		if err != nil {
			return err
		}
		fmt.Printf("clearing lock identifier [%s] expiry [%v]\n", lock.LockIdentifier, lock.Expiry)

		// update
		return trx.BufferWrite([]*spanner.Mutation{
			spanner.Update(tableName,
				[]string{"ID", "LockIdentifier", "Expiry"},
				[]interface{}{spanner.NullInt64{}, spanner.NullString{}, spanner.NullTime{}}),
		})
	})

	return err
}

func (c *Client) GetMigrationLock(ctx context.Context, tableName, lockIdentifier string) (lock MigrationLock, err error) {
	lock = MigrationLock{
		Release: func() {},
	}

	// skip if lock table not setup
	if !c.tableExists(ctx, tableName) {
		lock.Success = true
		return lock, err
	}
	_, err = c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, trx *spanner.ReadWriteTransaction) error {
		sql := fmt.Sprintf(`UPDATE %s SET LockIdentifier=@lockIdentifier, 
		Expiry = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE) 
		WHERE ID IS NULL AND (LockIdentifier IS NULL OR CURRENT_TIMESTAMP() > Expiry)`, tableName)
		lockStmt := spanner.NewStatement(sql)
		lockStmt.Params["lockIdentifier"] = lockIdentifier
		rc, err := trx.Update(ctx, lockStmt)
		if err != nil {
			return err
		}

		lock.Success = rc == 1

		sql2 := fmt.Sprintf("Select LockIdentifier, Expiry FROM %s WHERE ID IS NULL", tableName)
		err = trx.Query(ctx, spanner.NewStatement(sql2)).
			Do(func(r *spanner.Row) error {
				if err := r.ToStruct(&lock); err != nil {
					return err
				}
				return nil
			})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return lock, err
	}

	// fmt.Printf("%v %s %v\n", lock.Success, lock.LockIdentifier, lock.Expiry)

	lock.Release = func() {
		err = c.releaseMigrationLock(ctx, tableName, lockIdentifier)
		if err != nil {
			fmt.Printf("failed to release migration lock: %v\n", err)
		}
	}

	return lock, err
}

func (c *Client) releaseMigrationLock(ctx context.Context, tableName, lockIdentifier string) error {
	_, err := c.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, trx *spanner.ReadWriteTransaction) error {
		sql := fmt.Sprintf("Update %s SET LockIdentifier=NULL, Expiry=NULL WHERE ID IS NULL AND LockIdentifier=@lockIdentifier", tableName)
		stmt := spanner.NewStatement(sql)
		stmt.Params["lockIdentifier"] = lockIdentifier
		_, err := trx.Update(ctx, stmt)
		if err != nil {
			return err
		}
		// log.Printf("release migration lock %s %v\n", lockIdentifier, rc == 1)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
