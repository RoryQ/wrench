package bigquery

import (
	"cmp"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/roryq/wrench/pkg/core"
	"github.com/roryq/wrench/pkg/migration"
)

type recordedQuery struct {
	sql    string
	params []bq.QueryParameter
}

type fakeExecutor struct {
	tables            map[string]bool
	queries           []recordedQuery
	runQueryFn        func(sql string, params ...bq.QueryParameter) (int64, error)
	version           int64
	dirty             bool
	versionFound      bool
	lockID            string
	lockExpiry        time.Time
	lockFound         bool
	history              []migration.MigrationHistoryRecord
	repeatableHistory    []migration.RepeatableMigrationHistoryRecord
	querySchemaObjectsFn func(sql string) ([]schemaObjectRow, error)
	closed               bool
}

func (f *fakeExecutor) tableExists(_ context.Context, tableName string) (bool, error) {
	return f.tables[tableName], nil
}

func (f *fakeExecutor) runQuery(_ context.Context, sql string, params ...bq.QueryParameter) (int64, error) {
	f.queries = append(f.queries, recordedQuery{sql: sql, params: params})
	if f.runQueryFn != nil {
		return f.runQueryFn(sql, params...)
	}
	return 0, nil
}

func (f *fakeExecutor) queryVersion(_ context.Context, _ string, _ ...bq.QueryParameter) (int64, bool, bool, error) {
	return f.version, f.dirty, f.versionFound, nil
}

func (f *fakeExecutor) queryLock(_ context.Context, _ string, _ ...bq.QueryParameter) (string, time.Time, bool, error) {
	return f.lockID, f.lockExpiry, f.lockFound, nil
}

func (f *fakeExecutor) queryHistory(_ context.Context, _ string, _ ...bq.QueryParameter) ([]migration.MigrationHistoryRecord, error) {
	return f.history, nil
}

func (f *fakeExecutor) queryRepeatableHistory(_ context.Context, _ string, _ ...bq.QueryParameter) ([]migration.RepeatableMigrationHistoryRecord, error) {
	return f.repeatableHistory, nil
}

func (f *fakeExecutor) querySchemaObjects(_ context.Context, sql string) ([]schemaObjectRow, error) {
	f.queries = append(f.queries, recordedQuery{sql: sql})
	if f.querySchemaObjectsFn != nil {
		return f.querySchemaObjectsFn(sql)
	}
	return nil, nil
}

func (f *fakeExecutor) close() error {
	f.closed = true
	return nil
}

func newTestClient(exec *fakeExecutor) *Client {
	return &Client{
		config: &Config{
			Project:  "my-project",
			Dataset:  "my_dataset",
			Location: "US",
		},
		exec: exec,
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: "bigquery config is required",
		},
		{
			name:    "missing project",
			cfg:     &Config{Dataset: "ds"},
			wantErr: "bigquery project is required",
		},
		{
			name:    "missing dataset",
			cfg:     &Config{Project: "proj"},
			wantErr: "bigquery dataset is required",
		},
		{
			name:    "invalid project with backtick",
			cfg:     &Config{Project: "proj`; DROP TABLE x--", Dataset: "ds"},
			wantErr: "invalid bigquery project identifier",
		},
		{
			name:    "invalid dataset with space",
			cfg:     &Config{Project: "proj", Dataset: "my dataset"},
			wantErr: "invalid bigquery dataset identifier",
		},
		{
			name: "valid domain-scoped project",
			cfg:  &Config{Project: "example.com:my-proj", Dataset: "my_ds_1", Location: "US"},
		},
		{
			name: "valid config",
			cfg:  &Config{Project: "proj", Dataset: "ds", Location: "US"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigResolvedEndpoint(t *testing.T) {
	t.Run("explicit endpoint with scheme", func(t *testing.T) {
		cfg := &Config{Endpoint: "http://localhost:9050"}
		assert.Equal(t, "http://localhost:9050", cfg.resolvedEndpoint())
	})

	t.Run("explicit endpoint without scheme", func(t *testing.T) {
		cfg := &Config{Endpoint: "localhost:9050"}
		assert.Equal(t, "http://localhost:9050", cfg.resolvedEndpoint())
	})

	t.Run("from BIGQUERY_EMULATOR_HOST env var", func(t *testing.T) {
		t.Setenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
		cfg := &Config{}
		assert.Equal(t, "http://localhost:9050", cfg.resolvedEndpoint())
	})

	t.Run("empty when unset", func(t *testing.T) {
		t.Setenv("BIGQUERY_EMULATOR_HOST", "")
		cfg := &Config{}
		assert.Equal(t, "", cfg.resolvedEndpoint())
	})
}

func TestNewClientValidationError(t *testing.T) {
	client, err := NewClient(t.Context(), &Config{})
	require.Nil(t, client)
	require.Error(t, err)

	var bqErr *Error
	require.ErrorAs(t, err, &bqErr)
	assert.Equal(t, ErrorCodeCreateClient, bqErr.Code)
}

func TestErrorUnwrap(t *testing.T) {
	err := &Error{
		Code: ErrorCodeNoMigration,
		err:  migration.ErrNoMigration,
	}
	assert.True(t, errors.Is(err, migration.ErrNoMigration))
	assert.Equal(t, "No migration.", err.Error())
}

func TestHelpers(t *testing.T) {
	c := newTestClient(&fakeExecutor{})

	assert.True(t, c.UseRawScript())
	ref, err := c.tableRef("SchemaMigrations")
	require.NoError(t, err)
	assert.Equal(t, "`my-project.my_dataset.SchemaMigrations`", ref)

	_, err = c.tableRef("bad`table")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid bigquery table identifier")

	assert.Equal(t, "SchemaMigrationsHistory", c.historyTableName("SchemaMigrations"))

	assert.Equal(t, "SELECT 1;", migrationSQL(&migration.Migration{
		RawSQL:     "  SELECT 1;  ",
		Statements: []string{"SELECT 2"},
	}))
	assert.Equal(t, "SELECT 1;\nSELECT 2", migrationSQL(&migration.Migration{
		Statements: []string{"SELECT 1", "SELECT 2"},
	}))
}

func TestEnsureMigrationTable_FirstRunInitializesLock(t *testing.T) {
	exec := &fakeExecutor{tables: map[string]bool{}}
	c := newTestClient(exec)

	require.NoError(t, c.EnsureMigrationTable(t.Context(), "SchemaMigrations"))
	// Expect: CREATE SchemaMigrations, CREATE SchemaMigrationsHistory, CREATE SchemaMigrationsLock, MERGE lock row
	require.Len(t, exec.queries, 4)
	assert.Contains(t, exec.queries[0].sql, "SchemaMigrations`")
	assert.Contains(t, exec.queries[1].sql, "SchemaMigrationsHistory`")
	assert.Contains(t, exec.queries[2].sql, "SchemaMigrationsLock`")
	assert.Contains(t, exec.queries[3].sql, "MERGE `my-project.my_dataset.SchemaMigrationsLock`")
}

func TestGetMigrationLock(t *testing.T) {
	t.Run("skips when lock table does not exist", func(t *testing.T) {
		exec := &fakeExecutor{tables: map[string]bool{}}
		c := newTestClient(exec)

		lock, err := c.GetMigrationLock(t.Context(), "SchemaMigrationsLock", "proc-1")
		require.NoError(t, err)
		assert.True(t, lock.Success)
		lock.Release()
		assert.Empty(t, exec.queries)
	})

	t.Run("acquires and releases lock", func(t *testing.T) {
		expiry := time.Now().Add(30 * time.Minute)
		exec := &fakeExecutor{
			tables:     map[string]bool{"SchemaMigrationsLock": true},
			lockID:     "proc-1",
			lockExpiry: expiry,
			lockFound:  true,
			runQueryFn: func(sql string, _ ...bq.QueryParameter) (int64, error) {
				if strings.Contains(sql, "TIMESTAMP_ADD") {
					return 1, nil
				}
				return 1, nil
			},
		}
		c := newTestClient(exec)

		lock, err := c.GetMigrationLock(t.Context(), "SchemaMigrationsLock", "proc-1")
		require.NoError(t, err)
		assert.True(t, lock.Success)
		assert.Equal(t, "proc-1", lock.LockIdentifier)
		assert.Equal(t, expiry, lock.Expiry)

		lock.Release()
		require.Len(t, exec.queries, 2)
		assert.Contains(t, exec.queries[1].sql, "SET LockIdentifier = NULL, Expiry = NULL")
	})

	t.Run("returns unacquired lock when held by another process", func(t *testing.T) {
		expiry := time.Now().Add(15 * time.Minute)
		exec := &fakeExecutor{
			tables:     map[string]bool{"SchemaMigrationsLock": true},
			lockID:     "other-proc",
			lockExpiry: expiry,
			lockFound:  true,
			runQueryFn: func(_ string, _ ...bq.QueryParameter) (int64, error) {
				return 0, nil
			},
		}
		c := newTestClient(exec)

		lock, err := c.GetMigrationLock(t.Context(), "SchemaMigrationsLock", "proc-1")
		require.NoError(t, err)
		assert.False(t, lock.Success)
		assert.Equal(t, "other-proc", lock.LockIdentifier)
		assert.Equal(t, expiry, lock.Expiry)

		// Release should be a no-op when lock was not acquired
		lock.Release()
		require.Len(t, exec.queries, 1)
	})

	t.Run("errors clearly when lock table exists with no row", func(t *testing.T) {
		exec := &fakeExecutor{
			tables:    map[string]bool{"SchemaMigrationsLock": true},
			lockFound: false,
		}
		c := newTestClient(exec)

		_, err := c.GetMigrationLock(t.Context(), "SchemaMigrationsLock", "proc-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "setup-lock")
	})
}

func TestExecuteMigrations(t *testing.T) {
	t.Run("rejects dirty SchemaMigrations version", func(t *testing.T) {
		exec := &fakeExecutor{version: 2, dirty: true, versionFound: true}
		c := newTestClient(exec)

		err := c.ExecuteMigrations(t.Context(), migration.Migrations{
			{Version: 3, FileName: "000003.sql", RawSQL: "SELECT 1;"},
		}, migration.ExecuteOptions{Limit: -1, VersionTableName: "SchemaMigrations"})
		require.Error(t, err)

		var bqErr *Error
		require.ErrorAs(t, err, &bqErr)
		assert.Equal(t, ErrorCodeMigrationVersionDirty, bqErr.Code)
	})

	t.Run("rejects dirty SchemaMigrationsHistory record", func(t *testing.T) {
		exec := &fakeExecutor{
			versionFound: false,
			history:      []migration.MigrationHistoryRecord{{Version: 1, Dirty: true}},
		}
		c := newTestClient(exec)

		err := c.ExecuteMigrations(t.Context(), migration.Migrations{
			{Version: 1, FileName: "000001.sql", RawSQL: "SELECT 1;"},
		}, migration.ExecuteOptions{Limit: -1, VersionTableName: "SchemaMigrations"})
		require.Error(t, err)

		var bqErr *Error
		require.ErrorAs(t, err, &bqErr)
		assert.Equal(t, ErrorCodeMigrationVersionDirty, bqErr.Code)
	})

	t.Run("applies unapplied and out-of-order migrations respecting limit", func(t *testing.T) {
		exec := &fakeExecutor{
			version:      2,
			dirty:        false,
			versionFound: true,
			history:      []migration.MigrationHistoryRecord{{Version: 2, Dirty: false}},
			runQueryFn: func(sql string, _ ...bq.QueryParameter) (int64, error) {
				if strings.HasPrefix(sql, "INSERT INTO users") {
					return 3, nil
				}
				return 0, nil
			},
		}
		c := newTestClient(exec)

		err := c.ExecuteMigrations(t.Context(), migration.Migrations{
			{Version: 1, Name: "hotfix", FileName: "000001_hotfix.sql", RawSQL: "INSERT INTO users VALUES (1);"},
			{Version: 2, Name: "already_done", FileName: "000002_already_done.sql", RawSQL: "SELECT 2;"},
			{Version: 3, Name: "next", FileName: "000003_next.sql", RawSQL: "SELECT 3;"},
		}, migration.ExecuteOptions{Limit: 1, VersionTableName: "SchemaMigrations"})
		require.NoError(t, err)

		// Only version 1 should run (limit=1, version 2 already applied)
		require.Len(t, exec.queries, 3) // set dirty=true tx, migration SQL, set dirty=false tx
		assert.Contains(t, exec.queries[0].sql, "BEGIN TRANSACTION;")
		assert.Equal(t, "INSERT INTO users VALUES (1);", exec.queries[1].sql)
		assert.Contains(t, exec.queries[2].sql, "BEGIN TRANSACTION;")
	})
}

func TestExecuteRepeatableMigrations(t *testing.T) {
	t.Run("rejects non-repeatable migration", func(t *testing.T) {
		exec := &fakeExecutor{tables: map[string]bool{"SchemaMigrationsRepeatableHistory": true}}
		c := newTestClient(exec)

		err := c.ExecuteRepeatableMigrations(t.Context(), migration.Migrations{
			{Version: 1, FileName: "000001.sql", IsRepeatable: false},
		}, "SchemaMigrationsRepeatableHistory", migration.ExecuteOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a repeatable migration")
	})

	t.Run("skips unchanged checksum and applies changed repeatable migration", func(t *testing.T) {
		exec := &fakeExecutor{
			tables: map[string]bool{"SchemaMigrationsRepeatableHistory": true},
			repeatableHistory: []migration.RepeatableMigrationHistoryRecord{
				{Name: "unchanged", Checksum: "abc"},
				{Name: "updated", Checksum: "old"},
			},
		}
		c := newTestClient(exec)

		err := c.ExecuteRepeatableMigrations(t.Context(), migration.Migrations{
			{Name: "unchanged", FileName: "R__unchanged.sql", IsRepeatable: true, Checksum: "abc", RawSQL: "SELECT 1;"},
			{Name: "updated", FileName: "R__updated.sql", IsRepeatable: true, Checksum: "new", RawSQL: "CREATE OR REPLACE VIEW v AS SELECT 2;"},
		}, "SchemaMigrationsRepeatableHistory", migration.ExecuteOptions{})
		require.NoError(t, err)

		require.Len(t, exec.queries, 2)
		assert.Equal(t, "CREATE OR REPLACE VIEW v AS SELECT 2;", exec.queries[0].sql)
		assert.Contains(t, exec.queries[1].sql, "MERGE `my-project.my_dataset.SchemaMigrationsRepeatableHistory`")
	})
}

func TestRepairMigration(t *testing.T) {
	t.Run("deletes dirty history and restores previous clean version", func(t *testing.T) {
		exec := &fakeExecutor{version: 1, dirty: false, versionFound: true}
		c := newTestClient(exec)

		require.NoError(t, c.RepairMigration(t.Context(), "SchemaMigrations"))
		require.Len(t, exec.queries, 2)
		assert.Contains(t, exec.queries[0].sql, "DELETE FROM `my-project.my_dataset.SchemaMigrationsHistory` WHERE Dirty = TRUE")
		assert.Contains(t, exec.queries[1].sql, "BEGIN TRANSACTION;")
		assert.Contains(t, exec.queries[1].sql, "INSERT INTO `my-project.my_dataset.SchemaMigrations`")
	})

	t.Run("clears SchemaMigrations when no clean history remains", func(t *testing.T) {
		exec := &fakeExecutor{versionFound: false}
		c := newTestClient(exec)

		require.NoError(t, c.RepairMigration(t.Context(), "SchemaMigrations"))
		require.Len(t, exec.queries, 2)
		assert.Contains(t, exec.queries[0].sql, "DELETE FROM `my-project.my_dataset.SchemaMigrationsHistory` WHERE Dirty = TRUE")
		assert.Equal(t, "DELETE FROM `my-project.my_dataset.SchemaMigrations` WHERE TRUE", exec.queries[1].sql)
	})
}

func TestBigQueryMigrations_Emulator(t *testing.T) {
	if os.Getenv("BIGQUERY_EMULATOR_HOST") == "" {
		t.Skip("BIGQUERY_EMULATOR_HOST not set; skipping BigQuery emulator integration test")
	}

	ctx := t.Context()
	project := cmp.Or(os.Getenv("BIGQUERY_PROJECT_ID"), "test-project")
	dataset := cmp.Or(os.Getenv("BIGQUERY_DATASET_ID"), "test_dataset")

	client, err := NewClient(ctx, &Config{
		Project:     project,
		Dataset:     dataset,
		Location:    "US",
		StmtTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "000001_create_users.sql"),
		[]byte("CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.users` (id INT64, name STRING);"),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "R__users_view.sql"),
		[]byte("CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.v_users` AS SELECT id, name FROM `${PROJECT_ID}.${DATASET_ID}.users`;"),
		0o644,
	))

	require.NoError(t, core.MigrateSetupLockDriver(ctx, client))
	require.NoError(t, core.MigrateUpDriver(ctx, client, dir,
		core.WithBigQueryPlaceholders(true, project, dataset, "US"),
	))

	version, dirty, err := client.GetSchemaMigrationVersion(ctx, "SchemaMigrations")
	require.NoError(t, err)
	assert.Equal(t, uint(1), version)
	assert.False(t, dirty)

	history, err := client.GetMigrationHistory(ctx, "SchemaMigrations")
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, int64(1), history[0].Version)

	repHistory, err := client.GetRepeatableMigrationHistory(ctx, migration.RepeatableHistoryTableName("SchemaMigrations"))
	require.NoError(t, err)
	require.Len(t, repHistory, 1)
	assert.Equal(t, "users_view", repHistory[0].Name)
}
