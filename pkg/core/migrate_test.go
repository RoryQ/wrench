package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/roryq/wrench/pkg/migration"
)

type mockDriver struct {
	rawScript bool

	lockSuccess bool
	lockID      string
	lockExpiry  time.Time
	lockReleased bool

	ensuredVersionTable    string
	ensuredRepeatableTable string
	setupLockTable         string
	repairedTable          string

	executedVersioned  migration.Migrations
	executedRepeatable migration.Migrations
	lastExecOpts       migration.ExecuteOptions

	history           []migration.MigrationHistoryRecord
	repeatableHistory []migration.RepeatableMigrationHistoryRecord
}

func (m *mockDriver) UseRawScript() bool { return m.rawScript }

func (m *mockDriver) EnsureMigrationTable(_ context.Context, tableName string) error {
	m.ensuredVersionTable = tableName
	return nil
}

func (m *mockDriver) EnsureRepeatableMigrationTable(_ context.Context, tableName string) error {
	m.ensuredRepeatableTable = tableName
	return nil
}

func (m *mockDriver) SetupMigrationLock(_ context.Context, tableName string) error {
	m.setupLockTable = tableName
	return nil
}

func (m *mockDriver) GetMigrationLock(_ context.Context, _, _ string) (migration.MigrationLock, error) {
	return migration.MigrationLock{
		Success:        m.lockSuccess,
		LockIdentifier: m.lockID,
		Expiry:         m.lockExpiry,
		Release: func() {
			m.lockReleased = true
		},
	}, nil
}

func (m *mockDriver) ExecuteMigrations(_ context.Context, migrations migration.Migrations, opts migration.ExecuteOptions) error {
	m.executedVersioned = migrations
	m.lastExecOpts = opts
	return nil
}

func (m *mockDriver) ExecuteRepeatableMigrations(_ context.Context, migrations migration.Migrations, _ string, _ migration.ExecuteOptions) error {
	m.executedRepeatable = migrations
	return nil
}

func (m *mockDriver) GetSchemaMigrationVersion(_ context.Context, _ string) (uint, bool, error) {
	return 0, false, nil
}

func (m *mockDriver) GetMigrationHistory(_ context.Context, _ string) ([]migration.MigrationHistoryRecord, error) {
	return m.history, nil
}

func (m *mockDriver) GetRepeatableMigrationHistory(_ context.Context, _ string) ([]migration.RepeatableMigrationHistoryRecord, error) {
	return m.repeatableHistory, nil
}

func (m *mockDriver) RepairMigration(_ context.Context, tableName string) error {
	m.repairedTable = tableName
	return nil
}

func (m *mockDriver) Close() error { return nil }

func TestMigrateUpDriver_RawScriptAndBigQueryPlaceholders(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "000001_create_table.sql"),
		[]byte("CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.events` (id INT64);\nINSERT INTO `${PROJECT_ID}.${DATASET_ID}.events` VALUES (1);"),
		0o644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "R__events_view.sql"),
		[]byte("CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.v_events` AS SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.events`;"),
		0o644,
	))

	drv := &mockDriver{
		rawScript:   true,
		lockSuccess: true,
	}

	err := MigrateUpDriver(t.Context(), drv, dir,
		WithBigQueryPlaceholders(true, "proj-1", "ds_1", "US"),
		WithVersionTable("SchemaMigrations"),
		WithLimit(5),
	)
	require.NoError(t, err)

	assert.True(t, drv.lockReleased)
	assert.Equal(t, "SchemaMigrations", drv.ensuredVersionTable)
	assert.Equal(t, "SchemaMigrationsRepeatableHistory", drv.ensuredRepeatableTable)
	assert.Equal(t, 5, drv.lastExecOpts.Limit)

	require.Len(t, drv.executedVersioned, 1)
	assert.Contains(t, drv.executedVersioned[0].RawSQL, "`proj-1.ds_1.events`")

	require.Len(t, drv.executedRepeatable, 1)
	assert.Contains(t, drv.executedRepeatable[0].RawSQL, "`proj-1.ds_1.v_events`")
}

func TestMigrateUpDriver_LockContention(t *testing.T) {
	dir := t.TempDir()
	drv := &mockDriver{
		lockSuccess: false,
		lockID:      "other-process",
		lockExpiry:  time.Now().Add(10 * time.Minute),
	}

	err := MigrateUpDriver(t.Context(), drv, dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "other-process")
	assert.True(t, drv.lockReleased)
}

func TestMigrateRepairDriver(t *testing.T) {
	drv := &mockDriver{lockSuccess: true}
	err := MigrateRepairDriver(t.Context(), drv, WithVersionTable("CustomMigrations"))
	require.NoError(t, err)
	assert.Equal(t, "CustomMigrations", drv.ensuredVersionTable)
	assert.Equal(t, "CustomMigrations", drv.repairedTable)
	assert.True(t, drv.lockReleased)
}

func TestMigrateSetupLockDriver(t *testing.T) {
	drv := &mockDriver{}
	err := MigrateSetupLockDriver(t.Context(), drv, WithLockTable("CustomLock"))
	require.NoError(t, err)
	assert.Equal(t, "CustomLock", drv.setupLockTable)
}

func TestMigrateHistoryDriver(t *testing.T) {
	now := time.Now()
	drv := &mockDriver{
		lockSuccess: true,
		history: []migration.MigrationHistoryRecord{
			{Version: 1, Dirty: false, Created: now, Modified: now},
		},
		repeatableHistory: []migration.RepeatableMigrationHistoryRecord{
			{Name: "my_view", Checksum: "abcdef1234567890", AppliedAt: now},
		},
	}

	err := MigrateHistoryDriver(t.Context(), drv)
	require.NoError(t, err)
	assert.True(t, drv.lockReleased)
}

func TestCreateMigrationFile_MixedSQL(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "000010_init.sql"),
		[]byte("CREATE TABLE t (id INT64);\nINSERT INTO t VALUES (1);"),
		0o644,
	))

	filename, err := CreateMigrationFile(dir, "add_users", WithInterval(10), WithZeroPrefixLength(6))
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "000020_add_users.sql"), filename)
}
