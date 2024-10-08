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

	"github.com/roryq/wrench/pkg/spanner"
)

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

	migrationOptions := spanner.ExecuteMigrationOptions{
		Limit:                     options.Limit,
		VersionTableName:          options.VersionTableName,
		PartitionedDMLConcurrency: options.PartitionedDMLConcurrency,
		FastForward:               options.FastForward,
	}
	var migrationsOutput spanner.MigrationsOutput
	switch status {
	case spanner.ExistingMigrationsUpgradeStarted:
		migrationsOutput, err = client.UpgradeExecuteMigrations(ctx, migrations, migrationOptions)
		if err != nil {
			return err
		}
	case spanner.ExistingMigrationsUpgradeCompleted:
		migrationsOutput, err = client.ExecuteMigrations(ctx, migrations, migrationOptions)
		if err != nil {
			return err
		}
	default:
		return errors.New("migration in undetermined state")
	}
	if options.PrintRowsAffected {
		fmt.Print(migrationsOutput.String())
	}

	return nil
}

// MigrateHistory prints the migration history.
// The relevant options are LockTableName, LockIdentifier and VersionTableName.
func MigrateHistory(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
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

	history, err := client.GetMigrationHistory(ctx, options.VersionTableName)
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
	return nil
}

// MigrateRepair repairs the migration history table if it in a dirty state after a failed migration. After cleaning the
// schema manually run this step to remove the latest migration from the history table.
// The relevant options are LockTableName, LockIdentifier and VersionTableName.
func MigrateRepair(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
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

	if err = client.EnsureMigrationTable(ctx, options.VersionTableName); err != nil {
		return err
	}

	if err := client.RepairMigration(ctx, options.VersionTableName); err != nil {
		return err
	}

	return nil
}

// MigrateSetupLock sets up the migration lock table.
// The relevant options are LockTableName.
func MigrateSetupLock(ctx context.Context, client *spanner.Client, opts ...MigrateOpt) error {
	options := defaultMigrateOptions()
	for _, optFn := range opts {
		if err := optFn(options); err != nil {
			return err
		}
	}
	return client.SetupMigrationLock(ctx, options.LockTableName)
}
