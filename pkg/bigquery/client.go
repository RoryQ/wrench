package bigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/roryq/wrench/pkg/migration"
)

const (
	historyStr     = "History"
	lockStr        = "Lock"
	defaultLockKey = "default"
	lockTTLMinutes = 30
)

type queryExecutor interface {
	tableExists(ctx context.Context, tableName string) (bool, error)
	runQuery(ctx context.Context, sql string, params ...bq.QueryParameter) (int64, error)
	queryVersion(ctx context.Context, sql string, params ...bq.QueryParameter) (version int64, dirty bool, found bool, err error)
	queryLock(ctx context.Context, sql string, params ...bq.QueryParameter) (lockID string, expiry time.Time, found bool, err error)
	queryHistory(ctx context.Context, sql string, params ...bq.QueryParameter) ([]migration.MigrationHistoryRecord, error)
	queryRepeatableHistory(ctx context.Context, sql string, params ...bq.QueryParameter) ([]migration.RepeatableMigrationHistoryRecord, error)
	querySchemaObjects(ctx context.Context, sql string) ([]schemaObjectRow, error)
	close() error
}

type bqExecutor struct {
	config   *Config
	bqClient *bq.Client
}

func (e *bqExecutor) close() error {
	return e.bqClient.Close()
}

func (e *bqExecutor) tableExists(ctx context.Context, tableName string) (bool, error) {
	_, err := e.bqClient.Dataset(e.config.Dataset).Table(tableName).Metadata(ctx)
	if err == nil {
		return true, nil
	}
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok && gerr.Code == http.StatusNotFound {
		return false, nil
	}
	return false, err
}

func (e *bqExecutor) newQuery(sql string, params ...bq.QueryParameter) *bq.Query {
	q := e.bqClient.Query(sql)
	q.DefaultProjectID = e.config.Project
	q.DefaultDatasetID = e.config.Dataset
	if e.config.Location != "" {
		q.Location = e.config.Location
	}
	if len(params) > 0 {
		q.Parameters = params
	}
	return q
}

func (e *bqExecutor) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.config.StmtTimeout > 0 {
		return context.WithTimeout(ctx, e.config.StmtTimeout)
	}
	return ctx, func() {}
}

func (e *bqExecutor) runQuery(ctx context.Context, sql string, params ...bq.QueryParameter) (int64, error) {
	qctx, cancel := e.withTimeout(ctx)
	defer cancel()

	q := e.newQuery(sql, params...)
	job, err := q.Run(qctx)
	if err != nil {
		return 0, err
	}

	status, err := job.Wait(qctx)
	if err != nil {
		return 0, err
	}
	if err := status.Err(); err != nil {
		return 0, err
	}

	var rowsAffected int64
	if status.Statistics != nil {
		if qs, ok := status.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil {
			rowsAffected = qs.NumDMLAffectedRows
		}
	}
	return rowsAffected, nil
}

func (e *bqExecutor) queryRows(ctx context.Context, sql string, params ...bq.QueryParameter) (*bq.RowIterator, context.CancelFunc, error) {
	qctx, cancel := e.withTimeout(ctx)

	q := e.newQuery(sql, params...)
	it, err := q.Read(qctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return it, cancel, nil
}

func (e *bqExecutor) queryVersion(ctx context.Context, sql string, params ...bq.QueryParameter) (int64, bool, bool, error) {
	it, cancel, err := e.queryRows(ctx, sql, params...)
	if err != nil {
		return 0, false, false, err
	}
	defer cancel()

	var row struct {
		Version int64 `bigquery:"Version"`
		Dirty   bool  `bigquery:"Dirty"`
	}
	err = it.Next(&row)
	if errors.Is(err, iterator.Done) {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, err
	}
	return row.Version, row.Dirty, true, nil
}

func (e *bqExecutor) queryLock(ctx context.Context, sql string, params ...bq.QueryParameter) (string, time.Time, bool, error) {
	it, cancel, err := e.queryRows(ctx, sql, params...)
	if err != nil {
		return "", time.Time{}, false, err
	}
	defer cancel()

	var row struct {
		LockIdentifier bq.NullString    `bigquery:"LockIdentifier"`
		Expiry         bq.NullTimestamp `bigquery:"Expiry"`
	}
	err = it.Next(&row)
	if errors.Is(err, iterator.Done) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}

	var lockID string
	if row.LockIdentifier.Valid {
		lockID = row.LockIdentifier.StringVal
	}
	var expiry time.Time
	if row.Expiry.Valid {
		expiry = row.Expiry.Timestamp
	}
	return lockID, expiry, true, nil
}

func (e *bqExecutor) queryHistory(ctx context.Context, sql string, params ...bq.QueryParameter) ([]migration.MigrationHistoryRecord, error) {
	it, cancel, err := e.queryRows(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer cancel()

	var history []migration.MigrationHistoryRecord
	for {
		var rec migration.MigrationHistoryRecord
		err := it.Next(&rec)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		history = append(history, rec)
	}
	return history, nil
}

func (e *bqExecutor) queryRepeatableHistory(ctx context.Context, sql string, params ...bq.QueryParameter) ([]migration.RepeatableMigrationHistoryRecord, error) {
	it, cancel, err := e.queryRows(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer cancel()

	var history []migration.RepeatableMigrationHistoryRecord
	for {
		var rec migration.RepeatableMigrationHistoryRecord
		err := it.Next(&rec)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		history = append(history, rec)
	}
	return history, nil
}

func (e *bqExecutor) querySchemaObjects(ctx context.Context, sql string) ([]schemaObjectRow, error) {
	it, cancel, err := e.queryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer cancel()

	var rows []schemaObjectRow
	for {
		var row schemaObjectRow
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Client is a BigQuery migration driver implementing migration.Driver and migration.ScriptModeDriver.
type Client struct {
	config *Config
	exec   queryExecutor
}

var (
	_ migration.Driver           = (*Client)(nil)
	_ migration.ScriptModeDriver = (*Client)(nil)
)

// NewClient initializes a BigQuery migration client.
func NewClient(ctx context.Context, config *Config) (*Client, error) {
	if err := config.validate(); err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	var opts []option.ClientOption
	if ep := config.resolvedEndpoint(); ep != "" {
		opts = append(opts, option.WithEndpoint(ep), option.WithoutAuthentication())
	} else if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	bqClient, err := bq.NewClient(ctx, config.Project, opts...)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeCreateClient,
			err:  err,
		}
	}

	if config.Location != "" {
		bqClient.Location = config.Location
	}

	return &Client{
		config: config,
		exec: &bqExecutor{
			config:   config,
			bqClient: bqClient,
		},
	}, nil
}

// UseRawScript indicates that BigQuery executes full SQL scripts directly without semicolon splitting.
func (c *Client) UseRawScript() bool {
	return true
}

// Close closes the underlying BigQuery client.
func (c *Client) Close() error {
	if err := c.exec.close(); err != nil {
		return &Error{
			Code: ErrorCodeCloseClient,
			err:  err,
		}
	}
	return nil
}

func (c *Client) tableRef(tableName string) (string, error) {
	if err := validateIdentifier("table", tableName); err != nil {
		return "", err
	}
	return fmt.Sprintf("`%s.%s.%s`", c.config.Project, c.config.Dataset, tableName), nil
}

func (c *Client) historyTableName(versionTableName string) string {
	return versionTableName + historyStr
}

// EnsureMigrationTable ensures both the version table and history table exist in the BigQuery dataset.
// On first run (when the version table does not yet exist), it also initializes the migration lock table
// to match Spanner's default locking behavior.
func (c *Client) EnsureMigrationTable(ctx context.Context, tableName string) error {
	versionRef, err := c.tableRef(tableName)
	if err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}
	historyRef, err := c.tableRef(c.historyTableName(tableName))
	if err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}

	existed, err := c.exec.tableExists(ctx, tableName)
	if err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}

	createVersionSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		Version INT64 NOT NULL,
		Dirty BOOL NOT NULL
	)`, versionRef)
	if _, err := c.exec.runQuery(ctx, createVersionSQL); err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}

	createHistorySQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		Version INT64 NOT NULL,
		Dirty BOOL NOT NULL,
		Created TIMESTAMP NOT NULL,
		Modified TIMESTAMP NOT NULL
	)`, historyRef)
	if _, err := c.exec.runQuery(ctx, createHistorySQL); err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}

	if !existed {
		if err := c.SetupMigrationLock(ctx, tableName+lockStr); err != nil {
			return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
		}
	}

	return nil
}

// EnsureRepeatableMigrationTable ensures the repeatable migration history table exists.
func (c *Client) EnsureRepeatableMigrationTable(ctx context.Context, tableName string) error {
	ref, err := c.tableRef(tableName)
	if err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		Name STRING NOT NULL,
		Checksum STRING NOT NULL,
		AppliedAt TIMESTAMP NOT NULL
	)`, ref)
	if _, err := c.exec.runQuery(ctx, createSQL); err != nil {
		return &Error{Code: ErrorCodeEnsureMigrationTables, err: err}
	}
	return nil
}

// SetupMigrationLock initializes or resets the migration lock table.
func (c *Client) SetupMigrationLock(ctx context.Context, tableName string) error {
	ref, err := c.tableRef(tableName)
	if err != nil {
		return &Error{Code: ErrorCodeMigrationLock, err: err}
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		LockKey STRING NOT NULL,
		LockIdentifier STRING,
		Expiry TIMESTAMP
	)`, ref)
	if _, err := c.exec.runQuery(ctx, createSQL); err != nil {
		return &Error{Code: ErrorCodeMigrationLock, err: err}
	}

	mergeSQL := fmt.Sprintf(
		`MERGE %s T
		USING (SELECT @lockKey AS LockKey) S
		ON T.LockKey = S.LockKey
		WHEN MATCHED THEN
			UPDATE SET LockIdentifier = NULL, Expiry = NULL
		WHEN NOT MATCHED THEN
			INSERT (LockKey, LockIdentifier, Expiry) VALUES (S.LockKey, NULL, NULL)`,
		ref,
	)
	if _, err := c.exec.runQuery(ctx, mergeSQL, bq.QueryParameter{Name: "lockKey", Value: defaultLockKey}); err != nil {
		return &Error{Code: ErrorCodeMigrationLock, err: err}
	}

	return nil
}

// GetMigrationLock attempts to acquire a time-leased migration lock using conditional DML.
func (c *Client) GetMigrationLock(ctx context.Context, tableName, lockIdentifier string) (migration.MigrationLock, error) {
	lock := migration.MigrationLock{
		Release: func() {},
	}

	ref, err := c.tableRef(tableName)
	if err != nil {
		return lock, &Error{Code: ErrorCodeMigrationLock, err: err}
	}

	exists, err := c.exec.tableExists(ctx, tableName)
	if err != nil {
		return lock, &Error{Code: ErrorCodeMigrationLock, err: err}
	}
	if !exists {
		lock.Success = true
		return lock, nil
	}

	acquireSQL := fmt.Sprintf(
		`UPDATE %s
		SET LockIdentifier = @lockIdentifier,
		    Expiry = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL %d MINUTE)
		WHERE LockKey = @lockKey
		  AND (LockIdentifier IS NULL OR CURRENT_TIMESTAMP() > Expiry)`,
		ref,
		lockTTLMinutes,
	)
	rowsAffected, err := c.exec.runQuery(ctx, acquireSQL,
		bq.QueryParameter{Name: "lockIdentifier", Value: lockIdentifier},
		bq.QueryParameter{Name: "lockKey", Value: defaultLockKey},
	)
	if err != nil {
		return lock, &Error{Code: ErrorCodeMigrationLock, err: err}
	}

	lock.Success = rowsAffected == 1

	selectSQL := fmt.Sprintf(
		`SELECT LockIdentifier, Expiry FROM %s WHERE LockKey = @lockKey LIMIT 1`,
		ref,
	)
	holderID, expiry, found, err := c.exec.queryLock(ctx, selectSQL, bq.QueryParameter{Name: "lockKey", Value: defaultLockKey})
	if err != nil {
		return lock, &Error{Code: ErrorCodeMigrationLock, err: err}
	}
	if !found {
		return lock, &Error{
			Code: ErrorCodeMigrationLock,
			err:  fmt.Errorf("migration lock table %s is not initialized; run 'wrench migrate setup-lock'", tableName),
		}
	}
	lock.LockIdentifier = holderID
	lock.Expiry = expiry

	if lock.Success {
		lock.Release = func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer releaseCancel()
			if err := c.releaseMigrationLock(releaseCtx, ref, lockIdentifier); err != nil {
				slog.WarnContext(releaseCtx, "failed to release migration lock", "error", err)
			}
		}
	}

	return lock, nil
}

func (c *Client) releaseMigrationLock(ctx context.Context, tableRef, lockIdentifier string) error {
	releaseSQL := fmt.Sprintf(
		`UPDATE %s SET LockIdentifier = NULL, Expiry = NULL WHERE LockKey = @lockKey AND LockIdentifier = @lockIdentifier`,
		tableRef,
	)
	_, err := c.exec.runQuery(ctx, releaseSQL,
		bq.QueryParameter{Name: "lockKey", Value: defaultLockKey},
		bq.QueryParameter{Name: "lockIdentifier", Value: lockIdentifier},
	)
	return err
}

// GetSchemaMigrationVersion returns the latest recorded migration version and dirty status.
func (c *Client) GetSchemaMigrationVersion(ctx context.Context, tableName string) (uint, bool, error) {
	ref, err := c.tableRef(tableName)
	if err != nil {
		return 0, false, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}

	query := fmt.Sprintf(`SELECT Version, Dirty FROM %s ORDER BY Version DESC LIMIT 1`, ref)
	version, dirty, found, err := c.exec.queryVersion(ctx, query)
	if err != nil {
		return 0, false, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}
	if !found {
		return 0, false, &Error{
			Code: ErrorCodeNoMigration,
			err:  migration.ErrNoMigration,
		}
	}

	return uint(version), dirty, nil
}

func (c *Client) setSchemaMigrationVersion(ctx context.Context, version uint, dirty bool, tableName string) error {
	versionRef, err := c.tableRef(tableName)
	if err != nil {
		return &Error{Code: ErrorCodeSetMigrationVersion, err: err}
	}
	historyRef, err := c.tableRef(c.historyTableName(tableName))
	if err != nil {
		return &Error{Code: ErrorCodeSetMigrationVersion, err: err}
	}

	txSQL := fmt.Sprintf(
		`BEGIN TRANSACTION;
		DELETE FROM %s WHERE TRUE;
		INSERT INTO %s (Version, Dirty) VALUES (@version, @dirty);
		MERGE %s T
		USING (SELECT @version AS Version, @dirty AS Dirty) S
		ON T.Version = S.Version
		WHEN MATCHED THEN
			UPDATE SET Dirty = S.Dirty, Modified = CURRENT_TIMESTAMP()
		WHEN NOT MATCHED THEN
			INSERT (Version, Dirty, Created, Modified)
			VALUES (S.Version, S.Dirty, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
		COMMIT TRANSACTION;`,
		versionRef, versionRef, historyRef,
	)
	if _, err := c.exec.runQuery(ctx, txSQL,
		bq.QueryParameter{Name: "version", Value: int64(version)},
		bq.QueryParameter{Name: "dirty", Value: dirty},
	); err != nil {
		return &Error{Code: ErrorCodeSetMigrationVersion, err: err}
	}

	return nil
}

// GetMigrationHistory returns all applied versioned migration records.
func (c *Client) GetMigrationHistory(ctx context.Context, tableName string) ([]migration.MigrationHistoryRecord, error) {
	historyRef, err := c.tableRef(c.historyTableName(tableName))
	if err != nil {
		return nil, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}

	query := fmt.Sprintf(`SELECT Version, Dirty, Created, Modified FROM %s ORDER BY Version ASC`, historyRef)
	history, err := c.exec.queryHistory(ctx, query)
	if err != nil {
		return nil, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}
	return history, nil
}

// GetRepeatableMigrationHistory returns all applied repeatable migration records.
func (c *Client) GetRepeatableMigrationHistory(ctx context.Context, tableName string) ([]migration.RepeatableMigrationHistoryRecord, error) {
	ref, err := c.tableRef(tableName)
	if err != nil {
		return nil, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}

	exists, err := c.exec.tableExists(ctx, tableName)
	if err != nil {
		return nil, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}
	if !exists {
		return nil, nil
	}

	query := fmt.Sprintf(`SELECT Name, Checksum, AppliedAt FROM %s ORDER BY Name ASC`, ref)
	history, err := c.exec.queryRepeatableHistory(ctx, query)
	if err != nil {
		return nil, &Error{Code: ErrorCodeGetMigrationVersion, err: err}
	}
	return history, nil
}

// RepairMigration deletes all dirty rows from the migration history table and resets
// the version table to the latest non-dirty version (or empty if none remain).
func (c *Client) RepairMigration(ctx context.Context, tableName string) error {
	versionRef, err := c.tableRef(tableName)
	if err != nil {
		return &Error{Code: ErrorCodeRepairMigration, err: err}
	}
	historyRef, err := c.tableRef(c.historyTableName(tableName))
	if err != nil {
		return &Error{Code: ErrorCodeRepairMigration, err: err}
	}

	deleteDirtySQL := fmt.Sprintf(`DELETE FROM %s WHERE Dirty = TRUE`, historyRef)
	if _, err := c.exec.runQuery(ctx, deleteDirtySQL); err != nil {
		return &Error{Code: ErrorCodeRepairMigration, err: err}
	}

	latestSQL := fmt.Sprintf(`SELECT Version, Dirty FROM %s WHERE Dirty = FALSE ORDER BY Version DESC LIMIT 1`, historyRef)
	latestVersion, _, hasPrevious, err := c.exec.queryVersion(ctx, latestSQL)
	if err != nil {
		return &Error{Code: ErrorCodeRepairMigration, err: err}
	}

	if hasPrevious {
		resetSQL := fmt.Sprintf(
			`BEGIN TRANSACTION;
			DELETE FROM %s WHERE TRUE;
			INSERT INTO %s (Version, Dirty) VALUES (@version, FALSE);
			COMMIT TRANSACTION;`,
			versionRef, versionRef,
		)
		if _, err := c.exec.runQuery(ctx, resetSQL, bq.QueryParameter{Name: "version", Value: latestVersion}); err != nil {
			return &Error{Code: ErrorCodeRepairMigration, err: err}
		}
	} else {
		deleteVersionSQL := fmt.Sprintf(`DELETE FROM %s WHERE TRUE`, versionRef)
		if _, err := c.exec.runQuery(ctx, deleteVersionSQL); err != nil {
			return &Error{Code: ErrorCodeRepairMigration, err: err}
		}
	}

	return nil
}

// ExecuteMigrations applies all unapplied versioned migrations in order (up to opts.Limit).
func (c *Client) ExecuteMigrations(ctx context.Context, migrations migration.Migrations, opts migration.ExecuteOptions) error {
	sort.Sort(migrations)

	version, dirty, err := c.GetSchemaMigrationVersion(ctx, opts.VersionTableName)
	if err != nil && !errors.Is(err, migration.ErrNoMigration) {
		return &Error{Code: ErrorCodeExecuteMigrations, err: err}
	}
	if dirty {
		return &Error{
			Code: ErrorCodeMigrationVersionDirty,
			err:  fmt.Errorf("database version: %d is dirty, please fix it.", version),
		}
	}

	history, err := c.GetMigrationHistory(ctx, opts.VersionTableName)
	if err != nil {
		return &Error{Code: ErrorCodeExecuteMigrations, err: err}
	}

	applied := make(map[int64]bool, len(history))
	for i := range history {
		if history[i].Dirty {
			return &Error{
				Code: ErrorCodeMigrationVersionDirty,
				err:  fmt.Errorf("database version: %d is dirty, please fix it.", history[i].Version),
			}
		}
		applied[history[i].Version] = true
	}

	var count int

	for _, m := range migrations {
		if opts.Limit == 0 {
			break
		}
		if applied[int64(m.Version)] {
			continue
		}

		if err := c.setSchemaMigrationVersion(ctx, m.Version, true, opts.VersionTableName); err != nil {
			return &Error{Code: ErrorCodeExecuteMigrations, err: err}
		}

		var rowsPtr *int64
		sql := migrationSQL(m)
		if sql != "" {
			rowsAffected, err := c.exec.runQuery(ctx, sql)
			if err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  fmt.Errorf("migration %s failed: %w", m.FileName, err),
				}
			}
			if rowsAffected > 0 {
				rowsPtr = &rowsAffected
			}
		}

		m.LogApplied(ctx, rowsPtr)

		if err := c.setSchemaMigrationVersion(ctx, m.Version, false, opts.VersionTableName); err != nil {
			return &Error{Code: ErrorCodeExecuteMigrations, err: err}
		}

		count++
		if opts.Limit > 0 && count == opts.Limit {
			break
		}
	}

	if count == 0 {
		slog.InfoContext(ctx, "no change")
	}

	return nil
}

// ExecuteRepeatableMigrations executes repeatable migrations whose checksums are new or have changed.
func (c *Client) ExecuteRepeatableMigrations(ctx context.Context, migrations migration.Migrations, tableName string, _ migration.ExecuteOptions) error {
	if len(migrations) == 0 {
		return nil
	}

	for _, m := range migrations {
		if !m.IsRepeatable {
			return &Error{
				Code: ErrorCodeExecuteMigrations,
				err:  fmt.Errorf("migration %s is not a repeatable migration", m.FileName),
			}
		}
	}

	sort.Sort(migrations)

	history, err := c.GetRepeatableMigrationHistory(ctx, tableName)
	if err != nil {
		return &Error{Code: ErrorCodeExecuteMigrations, err: err}
	}

	appliedChecksums := make(map[string]string, len(history))
	for i := range history {
		appliedChecksums[history[i].Name] = history[i].Checksum
	}

	for _, m := range migrations {
		if existingChecksum, ok := appliedChecksums[m.Name]; ok && existingChecksum == m.Checksum {
			continue
		}

		var rowsPtr *int64
		sql := migrationSQL(m)
		if sql != "" {
			rowsAffected, err := c.exec.runQuery(ctx, sql)
			if err != nil {
				return &Error{
					Code: ErrorCodeExecuteMigrations,
					err:  fmt.Errorf("repeatable migration %s failed: %w", m.FileName, err),
				}
			}
			if rowsAffected > 0 {
				rowsPtr = &rowsAffected
			}
		}

		if err := c.upsertRepeatableHistory(ctx, tableName, m.Name, m.Checksum); err != nil {
			return &Error{Code: ErrorCodeExecuteMigrations, err: err}
		}

		m.LogApplied(ctx, rowsPtr)
	}

	return nil
}

func (c *Client) upsertRepeatableHistory(ctx context.Context, tableName, name, checksum string) error {
	ref, err := c.tableRef(tableName)
	if err != nil {
		return err
	}

	mergeSQL := fmt.Sprintf(
		`MERGE %s T
		USING (SELECT @name AS Name, @checksum AS Checksum) S
		ON T.Name = S.Name
		WHEN MATCHED THEN
			UPDATE SET Checksum = S.Checksum, AppliedAt = CURRENT_TIMESTAMP()
		WHEN NOT MATCHED THEN
			INSERT (Name, Checksum, AppliedAt) VALUES (S.Name, S.Checksum, CURRENT_TIMESTAMP())`,
		ref,
	)
	_, err = c.exec.runQuery(ctx, mergeSQL,
		bq.QueryParameter{Name: "name", Value: name},
		bq.QueryParameter{Name: "checksum", Value: checksum},
	)
	return err
}

func migrationSQL(m *migration.Migration) string {
	if m.RawSQL != "" {
		return strings.TrimSpace(m.RawSQL)
	}
	return strings.TrimSpace(strings.Join(m.Statements, ";\n"))
}
