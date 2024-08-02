package core

import "github.com/google/uuid"

type migrateOptions struct {
	// LockTableName is the name of the table that stores the lock.
	LockTableName string
	// VersionTableName is the name of the table that stores the version.
	VersionTableName string
	// LockIdentifier is the identifier of the lock holder when the lock is taken.
	LockIdentifier string
	// SkipVersions is a list of versions that should be skipped.
	SkipVersions []uint
	// Limit is the maximum number of migrations to apply.
	Limit int
	// PartitionedDMLConcurrency is the concurrency level for applying partitioned DML statements.
	PartitionedDMLConcurrency int
	// DetectPartitionedDML is whether to detect partitioned DML statements for use with the PartitionedDML API.
	DetectPartitionedDML bool
	// PrintRowsAffected is whether to print the number of rows affected by each migration.
	PrintRowsAffected bool
}

func defaultMigrateOptions() *migrateOptions {
	return &migrateOptions{
		LockIdentifier:       uuid.New().String(),
		SkipVersions:         nil,
		LockTableName:        "SchemaMigrationsLock",
		VersionTableName:     "SchemaMigrations",
		Limit:                -1,
		DetectPartitionedDML: false,
	}
}

// MigrateOpt is an option for Migrate functions.
type MigrateOpt func(opt *migrateOptions) error

// WithLockIdentifier sets the identifier of the lock holder when the lock is taken.
func WithLockIdentifier(lockIdentifier string) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.LockIdentifier = lockIdentifier
		return nil
	}
}

// WithSkipVersions sets a list of versions that should be skipped.
func WithSkipVersions(skipVersions []uint) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.SkipVersions = skipVersions
		return nil
	}
}

// WithLockTable sets the name of the table that stores the lock.
func WithLockTable(name string) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.LockTableName = name
		return nil
	}
}

// WithLimit sets the maximum number of migrations to apply.
func WithLimit(limit int) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.Limit = limit
		return nil
	}
}

// WithVersionTable sets the name of the table that stores the version.
func WithVersionTable(name string) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.VersionTableName = name
		return nil
	}
}

// WithDetectPartitionedDML sets whether to detect partitioned DML statements for use with the PartitionedDML API.
func WithDetectPartitionedDML(val bool) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.DetectPartitionedDML = val
		return nil
	}
}

// WithPartitionedDMLConcurrency sets the concurrency level for applying partitioned DML statements.
func WithPartitionedDMLConcurrency[T int | uint16](concurrency T) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.PartitionedDMLConcurrency = int(concurrency)
		return nil
	}
}

// WithPrintRowsAffected sets whether to print the number of rows affected by each migration.
func WithPrintRowsAffected(val bool) MigrateOpt {
	return func(opt *migrateOptions) error {
		opt.PrintRowsAffected = val
		return nil
	}
}

type migrationSequenceOptions struct {
	// Interval is the interval between the migration sequences.
	Interval uint
	// ZeroPrefixLength is the length of the zero digit prefix for the migration sequence.
	ZeroPrefixLength int
}

func defaultSequenceOptions() migrationSequenceOptions {
	return migrationSequenceOptions{
		Interval:         10,
		ZeroPrefixLength: 6,
	}
}

// MigrationSequenceOpt sets an option for the migration sequence.
type MigrationSequenceOpt func(opt *migrationSequenceOptions) error

// WithInterval sets the interval for the migration sequence.
// By default, the next migration sequence will be multiples of 10. This allows hotfix / out of order migrations to be
// applied to the database.
func WithInterval[T int | uint | uint16](interval T) MigrationSequenceOpt {
	return func(opt *migrationSequenceOptions) error {
		opt.Interval = uint(interval)
		return nil
	}
}

// WithZeroPrefixLength returns a MigrationSequenceOpt function that sets zero digit prefix length for the migration sequence.
func WithZeroPrefixLength(length int) MigrationSequenceOpt {
	return func(opt *migrationSequenceOptions) error {
		opt.ZeroPrefixLength = length
		return nil
	}
}
