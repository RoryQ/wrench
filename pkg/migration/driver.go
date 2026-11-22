package migration

import (
	"context"
	"errors"
	"time"
)

// ErrNoMigration is returned by Driver.GetSchemaMigrationVersion when no migrations have been applied yet.
var ErrNoMigration = errors.New("No migration.")

const repeatableHistoryStr = "RepeatableHistory"

// MigrationHistoryRecord represents a single applied versioned migration record.
type MigrationHistoryRecord struct {
	Version  int64     `spanner:"Version" bigquery:"Version"`
	Dirty    bool      `spanner:"Dirty" bigquery:"Dirty"`
	Created  time.Time `spanner:"Created" bigquery:"Created"`
	Modified time.Time `spanner:"Modified" bigquery:"Modified"`
}

// RepeatableMigrationHistoryRecord represents an applied repeatable migration record.
type RepeatableMigrationHistoryRecord struct {
	Name      string    `spanner:"Name" bigquery:"Name"`
	Checksum  string    `spanner:"Checksum" bigquery:"Checksum"`
	AppliedAt time.Time `spanner:"AppliedAt" bigquery:"AppliedAt"`
}

// RepeatableHistoryTableName returns the repeatable migration history table name
// derived from the base version table name.
func RepeatableHistoryTableName(versionTableName string) string {
	return versionTableName + repeatableHistoryStr
}

// MigrationLock represents an acquired (or contested) migration lock.
type MigrationLock struct {
	Success        bool
	Release        func()
	LockIdentifier string    `spanner:"LockIdentifier" bigquery:"LockIdentifier"`
	Expiry         time.Time `spanner:"Expiry" bigquery:"Expiry"`
}

// ExecuteOptions holds execution parameters passed from the core runner to a Driver.
type ExecuteOptions struct {
	Limit                     int
	VersionTableName          string
	PartitionedDMLConcurrency int
	ProtoDescriptors          []byte
	FFMigrations              bool
}

// Driver defines the database-specific operations required by the migration engine.
type Driver interface {
	// Tracking tables
	EnsureMigrationTable(ctx context.Context, tableName string) error
	EnsureRepeatableMigrationTable(ctx context.Context, tableName string) error

	// Locking
	SetupMigrationLock(ctx context.Context, tableName string) error
	GetMigrationLock(ctx context.Context, tableName, lockIdentifier string) (MigrationLock, error)

	// Execution
	ExecuteMigrations(ctx context.Context, migrations Migrations, opts ExecuteOptions) error
	ExecuteRepeatableMigrations(ctx context.Context, migrations Migrations, tableName string, opts ExecuteOptions) error

	// Inspection & Repair
	GetSchemaMigrationVersion(ctx context.Context, tableName string) (version uint, dirty bool, err error)
	GetMigrationHistory(ctx context.Context, tableName string) ([]MigrationHistoryRecord, error)
	GetRepeatableMigrationHistory(ctx context.Context, tableName string) ([]RepeatableMigrationHistoryRecord, error)
	RepairMigration(ctx context.Context, tableName string) error

	Close() error
}

// ScriptModeDriver is an optional interface that a Driver can implement if it
// executes raw SQL scripts (such as BigQuery procedural SQL) rather than
// semicolon-split statements.
type ScriptModeDriver interface {
	UseRawScript() bool
}
