package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/roryq/wrench/pkg/spanner"
)

type migrationSequenceOptions struct {
	Interval    uint
	DigitLength int
}

func defaultSequenceOptions() migrationSequenceOptions {
	return migrationSequenceOptions{
		Interval:    10,
		DigitLength: 6,
	}
}

type MigrationSequenceOpt func(opt *migrationSequenceOptions) error

func WithInterval[T int | uint | uint16](interval T) MigrationSequenceOpt {
	return func(opt *migrationSequenceOptions) error {
		opt.Interval = uint(interval)
		return nil
	}
}

func WithDigitLength(length int) MigrationSequenceOpt {
	return func(opt *migrationSequenceOptions) error {
		opt.DigitLength = length
		return nil
	}
}

func CreateMigrationFile(dir string, name string, opts ...MigrationSequenceOpt) (string, error) {
	options := defaultSequenceOptions()
	for _, optFn := range opts {
		if err := optFn(&options); err != nil {
			return "", err
		}
	}

	if name != "" && !spanner.MigrationNameRegex.MatchString(name) {
		return "", errors.New("Invalid migration file name.")
	}

	ms, err := spanner.LoadMigrations(dir, nil, false)
	if err != nil {
		return "", err
	}

	var v uint = 1
	if len(ms) > 0 {
		v = roundNext(ms[len(ms)-1].Version, options.Interval)
	}

	vStr := fmt.Sprintf("%0*d", options.DigitLength, v)

	var filename string
	if name == "" {
		filename = filepath.Join(dir, fmt.Sprintf("%s.sql", vStr))
	} else {
		filename = filepath.Join(dir, fmt.Sprintf("%s_%s.sql", vStr, name))
	}

	fp, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	_ = fp.Close()

	return filename, nil
}

func roundNext(n, next uint) uint {
	return uint(math.Round(float64(n)/float64(next)))*next + next
}

type migrationOptions struct {
	LockTableName             string
	VersionTableName          string
	LockIdentifier            string
	SkipVersions              []uint
	Limit                     int
	PartitionedDMLConcurrency int
	DetectPartitionedDML      bool
}

type MigrateOpt func(opt *migrationOptions) error

func WithLockIdentifier(lockIdentifier string) MigrateOpt {
	return func(opt *migrationOptions) error {
		opt.LockIdentifier = lockIdentifier
		return nil
	}
}

func WithSkipVersions(skipVersions []uint) MigrateOpt {
	return func(opt *migrationOptions) error {
		opt.SkipVersions = skipVersions
		return nil
	}
}

func WithLockTable(name string) MigrateOpt {
	return func(opt *migrationOptions) error {
		opt.LockTableName = name
		return nil
	}
}

func WithLimit(limit int) MigrateOpt {
	return func(opt *migrationOptions) error {
		opt.Limit = limit
		return nil
	}
}

func defaultMigrateOptions() *migrationOptions {
	return &migrationOptions{
		LockIdentifier:       uuid.New().String(),
		SkipVersions:         nil,
		LockTableName:        "SchemaMigrationsLock",
		VersionTableName:     "SchemaMigrations",
		Limit:                -1,
		DetectPartitionedDML: false,
	}
}

func MigrateUp(ctx context.Context, client *spanner.Client, migrationsDir string, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}

	lock, err := client.GetMigrationLock(ctx, options.LockTableName, options.LockIdentifier)
	defer lock.Release()
	if err != nil {
		return err
	}
	if !lock.Success {
		return fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry)
	}

	migrations, err := spanner.LoadMigrations(migrationsDir, options.SkipVersions, options.DetectPartitionedDML)
	if err != nil {
		return err
	}

	if err = client.EnsureMigrationTable(ctx, options.VersionTableName); err != nil {
		return err
	}

	status, err := client.DetermineUpgradeStatus(ctx, options.VersionTableName)
	if err != nil {
		return err
	}

	var migrationsOutput spanner.MigrationsOutput
	switch status {
	case spanner.ExistingMigrationsUpgradeStarted:
		migrationsOutput, err = client.UpgradeExecuteMigrations(ctx, migrations, options.Limit, options.VersionTableName)
		if err != nil {
			return err
		}
	case spanner.ExistingMigrationsUpgradeCompleted:
		migrationsOutput, err = client.ExecuteMigrations(ctx, migrations, options.Limit, options.VersionTableName, options.PartitionedDMLConcurrency)
		if err != nil {
			return err
		}
	default:
		return errors.New("migration in undetermined state")
	}
	if false {
		fmt.Print(migrationsOutput.String())
	}

	return nil
}
