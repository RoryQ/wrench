package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/roryq/wrench/pkg/migration"
	"github.com/roryq/wrench/pkg/spanner"
)

// Driver is the database-agnostic migration driver interface.
type Driver = migration.Driver

// CreateMigrationFile creates a new migration file in the given directory. The name should be alphanumeric with underscores
// or dashes only.
// The sequence options configure how the migration sequence is created.
func CreateMigrationFile(dir string, name string, opts ...MigrationSequenceOpt) (string, error) {
	options := defaultSequenceOptions()
	for _, optFn := range opts {
		if err := optFn(&options); err != nil {
			return "", err
		}
	}

	if name != "" && !migration.MigrationNameRegex.MatchString(name) {
		return "", errors.New("Invalid migration file name.")
	}

	ms, err := migration.LoadMigrationsWithOptions(dir, migration.LoadOptions{
		RawScript: true,
	})
	if err != nil {
		return "", err
	}

	var versionedMigrations migration.Migrations
	for _, m := range ms {
		if !m.IsRepeatable {
			versionedMigrations = append(versionedMigrations, m)
		}
	}

	var v uint = 1
	if len(versionedMigrations) > 0 {
		v = roundNext(versionedMigrations[len(versionedMigrations)-1].Version, options.Interval)
	}

	vStr := fmt.Sprintf("%0*d", options.ZeroPrefixLength, v)

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

// MigrateUp runs all migrations that haven't been run yet based on the contents of the history table.
func MigrateUp(ctx context.Context, client *spanner.Client, migrationsDir string, opts ...MigrateOpt) error {
	return MigrateUpDriver(ctx, NewSpannerDriver(client), migrationsDir, opts...)
}

// MigrateUpDriver runs all unapplied migrations against any Driver implementation.
func MigrateUpDriver(ctx context.Context, driver Driver, migrationsDir string, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}

	lock, err := driver.GetMigrationLock(ctx, options.LockTableName, options.LockIdentifier)
	if lock.Release != nil {
		defer lock.Release()
	}
	if err != nil {
		return err
	}
	if !lock.Success {
		return fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry)
	}

	rawScript := false
	if sd, ok := driver.(migration.ScriptModeDriver); ok {
		rawScript = sd.UseRawScript()
	}

	migrations, err := migration.LoadMigrationsWithOptions(migrationsDir, migration.LoadOptions{
		SkipVersions:         options.SkipVersions,
		DetectPartitionedDML: options.DetectPartitionedDML,
		Placeholders: migration.PlaceholderOptions{
			Placeholders:       options.Placeholders,
			ReplacementEnabled: options.PlaceholdersEnabled,
		},
		RawScript: rawScript,
	})
	if err != nil {
		return err
	}

	var versionedMigrations, repeatableMigrations migration.Migrations
	for _, m := range migrations {
		if m.IsRepeatable {
			repeatableMigrations = append(repeatableMigrations, m)
		} else {
			versionedMigrations = append(versionedMigrations, m)
		}
	}

	if err = driver.EnsureMigrationTable(ctx, options.VersionTableName); err != nil {
		return err
	}

	execOpts := migration.ExecuteOptions{
		Limit:                     options.Limit,
		VersionTableName:          options.VersionTableName,
		PartitionedDMLConcurrency: options.PartitionedDMLConcurrency,
		ProtoDescriptors:          options.ProtoDescriptors,
		FFMigrations:              options.FFMigrations,
	}

	if err := driver.ExecuteMigrations(ctx, versionedMigrations, execOpts); err != nil {
		return err
	}

	if len(repeatableMigrations) > 0 {
		repeatableTableName := migration.RepeatableHistoryTableName(options.VersionTableName)
		if err := driver.EnsureRepeatableMigrationTable(ctx, repeatableTableName); err != nil {
			return err
		}
		if err := driver.ExecuteRepeatableMigrations(ctx, repeatableMigrations, repeatableTableName, execOpts); err != nil {
			return err
		}
	}

	return nil
}

// MigrateHistory prints the migration history.
// The relevant options are LockTableName, LockIdentifier and VersionTableName.
func MigrateHistory(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
	return MigrateHistoryDriver(ctx, NewSpannerDriver(client), opts...)
}

// MigrateHistoryDriver prints the migration history for any Driver implementation.
func MigrateHistoryDriver(ctx context.Context, driver Driver, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}
	lock, err := driver.GetMigrationLock(ctx, options.LockTableName, options.LockIdentifier)
	if lock.Release != nil {
		defer lock.Release()
	}
	if err != nil {
		return err
	}
	if !lock.Success {
		return fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry)
	}

	history, err := driver.GetMigrationHistory(ctx, options.VersionTableName)
	if err != nil {
		return err
	}
	sort.SliceStable(history, func(i, j int) bool {
		return history[i].Created.Before(history[j].Created) // order by Created
	})

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	_, _ = fmt.Fprintln(writer, "Version\tDirty\tCreated\tModified")
	for i := range history {
		h := history[i]
		_, _ = fmt.Fprintf(writer, "%d\t%v\t%v\t%v\n", h.Version, h.Dirty, h.Created, h.Modified)
	}
	_ = writer.Flush()

	repeatableHistory, err := driver.GetRepeatableMigrationHistory(ctx, migration.RepeatableHistoryTableName(options.VersionTableName))
	if err != nil {
		return err
	}
	if len(repeatableHistory) > 0 {
		sort.SliceStable(repeatableHistory, func(i, j int) bool {
			return repeatableHistory[i].AppliedAt.Before(repeatableHistory[j].AppliedAt)
		})

		_, _ = fmt.Fprintln(os.Stdout)
		writer = tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
		_, _ = fmt.Fprintln(writer, "Repeatable Name\tChecksum\tApplied At")
		for i := range repeatableHistory {
			h := repeatableHistory[i]
			_, _ = fmt.Fprintf(writer, "%s\t%.8s\t%v\n", h.Name, h.Checksum, h.AppliedAt)
		}
		_ = writer.Flush()
	}

	return nil
}

// MigrateRepair repairs the migration history table if it in a dirty state after a failed migration. After cleaning the
// schema manually run this step to remove the latest migration from the history table.
// The relevant options are LockTableName, LockIdentifier and VersionTableName.
func MigrateRepair(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
	return MigrateRepairDriver(ctx, NewSpannerDriver(client), opts...)
}

// MigrateRepairDriver repairs the migration history table for any Driver implementation.
func MigrateRepairDriver(ctx context.Context, driver Driver, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}
	lock, err := driver.GetMigrationLock(ctx, options.LockTableName, options.LockIdentifier)
	if lock.Release != nil {
		defer lock.Release()
	}
	if err != nil {
		return err
	}
	if !lock.Success {
		return fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry)
	}

	if err = driver.EnsureMigrationTable(ctx, options.VersionTableName); err != nil {
		return err
	}

	if err := driver.RepairMigration(ctx, options.VersionTableName); err != nil {
		return err
	}

	return nil
}

// MigrateSetupLock sets up the migration lock table.
// The relevant options are LockTableName.
func MigrateSetupLock(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
	return MigrateSetupLockDriver(ctx, NewSpannerDriver(client), opts...)
}

// MigrateSetupLockDriver sets up the migration lock table for any Driver implementation.
func MigrateSetupLockDriver(ctx context.Context, driver Driver, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}
	return driver.SetupMigrationLock(ctx, options.LockTableName)
}
