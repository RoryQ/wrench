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

package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kennygrant/sanitize"
	"github.com/spf13/cobra"

	"github.com/roryq/wrench/internal/fs"

	"github.com/roryq/wrench/pkg/core"
	"github.com/roryq/wrench/pkg/migration"
)

const (
	migrationsDirName  = "migrations"
	migrationTableName = "SchemaMigrations"
	migrationLockTable = migrationTableName + "Lock"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate database",
}

func init() {
	migrateCreateCmd := &cobra.Command{
		Use:   "create NAME",
		Short: "Create a set of sequential up migrations in directory",
		RunE:  migrateCreate,
	}
	migrateUpCmd := &cobra.Command{
		Use:   "up [N]",
		Short: "Apply all or N up migrations",
		RunE:  migrateUp,
	}
	migrateVersionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print current migration version",
		RunE:  migrateVersion,
	}
	migrateSetCmd := &cobra.Command{
		Deprecated: "If you need to clean a dirty migration run `wrench migrate repair`",
		Hidden:     true,
	}
	migrateRepairCmd := &cobra.Command{
		Use:   "repair",
		Short: "If a migration has failed, clean up any schema changes manually then repair the history with this command",
		RunE:  migrateRepair,
	}
	migrateHistoryCmd := &cobra.Command{
		Use:   "history",
		Short: "Print migration version history",
		RunE:  migrateHistory,
	}
	migrateLockerCmd := &cobra.Command{
		Use:   "setup-lock",
		Short: "Initialise or reset the migration lock",
		Long:  "Call once to enable the migration lock. Call again to reset the lock if a failure caused it not to release",
		RunE:  migrateLocker,
	}

	migrateCmd.AddCommand(
		migrateCreateCmd,
		migrateUpCmd,
		migrateVersionCmd,
		migrateSetCmd,
		migrateHistoryCmd,
		migrateLockerCmd,
		migrateRepairCmd,
	)

	migrateCreateCmd.Flags().SetNormalizeFunc(underscoreToDashes)
	migrateUpCmd.Flags().SetNormalizeFunc(underscoreToDashes)

	migrateCreateCmd.Flags().Bool(flagNameCreateNoPrompt, false, "Don't prompt for a migration file description")
	migrateUpCmd.Flags().UintSlice(flagSkipVersions, []uint{}, "Versions to skip during migration")
	migrateUpCmd.Flags().Bool(flagPlaceholderReplacement, true, "Enable placeholder replacement (Spanner: ${PROJECT_ID}, ${INSTANCE_ID}, ${DATABASE_ID}; BigQuery: ${PROJECT_ID}, ${DATASET_ID}, ${LOCATION})")
	migrateUpCmd.Flags().String(flagProtoDescriptorFile, "", "Proto descriptor file to be used with migrations")
	migrateUpCmd.Flags().Bool(flagFFMigrations, false, "Fast-forward migrations by batching contiguous DDL migrations into a request. Intended for dev environments.")
}

func migrateCreate(c *cobra.Command, args []string) error {
	name := getNameForMigration(c, args)

	dir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return &Error{
				cmd: c,
				err: err,
			}
		}
	}

	filename, err := core.CreateMigrationFile(dir, name, core.WithInterval(sequenceInterval), core.WithZeroPrefixLength(6))
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	fmt.Printf("%s is created\n", filename)

	return nil
}

func getNameForMigration(c *cobra.Command, args []string) string {
	name := ""

	noPrompt := c.Flag(flagNameCreateNoPrompt).Value.String() == "true"
	// use argument as name if provided
	if len(args) > 0 {
		name = args[0]
	} else if !noPrompt {
		name = promptDescription()
	}
	return name
}

func migrateUp(c *cobra.Command, args []string) error {
	ctx := context.Background()

	limit := -1
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return &Error{
				cmd: c,
				err: err,
			}
		}
		limit = n
	}

	toSkip, err := c.Flags().GetUintSlice(flagSkipVersions)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	drv, driverName, err := newMigrationDriver(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = drv.Close() }()

	placeholdersEnabled, err := c.Flags().GetBool(flagPlaceholderReplacement)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	opts := []core.MigrateOpt{
		core.WithLimit(limit),
		core.WithSkipVersions(toSkip),
		core.WithLockIdentifier(lockIdentifier),
		core.WithVersionTable(migrationTableName),
		core.WithLockTable(migrationLockTable),
		core.WithPrintRowsAffected(verbose),
	}

	switch driverName {
	case driverBigQuery:
		if c.Flags().Changed(flagFFMigrations) {
			return &Error{
				cmd: c,
				err: fmt.Errorf("--%s is not supported for the bigquery driver", flagFFMigrations),
			}
		}
		if c.Flags().Changed(flagProtoDescriptorFile) {
			return &Error{
				cmd: c,
				err: fmt.Errorf("--%s is not supported for the bigquery driver", flagProtoDescriptorFile),
			}
		}
		opts = append(opts,
			core.WithBigQueryPlaceholders(
				placeholdersEnabled,
				resolvedBigQueryProject(c),
				c.Flag(flagNameDataset).Value.String(),
				c.Flag(flagNameLocation).Value.String(),
			),
		)
	default:
		var protoDescriptor []byte
		protoDescriptorFile := protoDescriptorFilePath(c)
		if protoDescriptorFile != "" {
			protoDescriptor, err = fs.ReadFile(ctx, protoDescriptorFile)
			if err != nil {
				return &Error{
					err: err,
					cmd: c,
				}
			}
		}

		ffMigrations, err := c.Flags().GetBool(flagFFMigrations)
		if err != nil {
			return &Error{
				cmd: c,
				err: err,
			}
		}

		opts = append(opts,
			core.WithPartitionedDMLConcurrency(partitionedDMLConcurrency),
			core.WithDetectPartitionedDML(detectPartitionedDML),
			core.WithDefaultPlaceholders(
				placeholdersEnabled,
				c.Flag(flagNameProject).Value.String(),
				c.Flag(flagNameInstance).Value.String(),
				c.Flag(flagNameDatabase).Value.String(),
			),
			core.WithProtoDescriptors(protoDescriptor),
			core.WithFFMigrations(ffMigrations),
		)
	}

	migrationsDir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)
	err = core.MigrateUpDriver(ctx, drv, migrationsDir, opts...)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	return nil
}

func migrateVersion(c *cobra.Command, args []string) error {
	ctx := context.Background()

	drv, _, err := newMigrationDriver(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = drv.Close() }()

	if err = drv.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	v, _, err := drv.GetSchemaMigrationVersion(ctx, migrationTableName)
	if err != nil {
		if errors.Is(err, migration.ErrNoMigration) {
			fmt.Println("No migrations.")
			return nil
		}
		return &Error{
			cmd: c,
			err: err,
		}
	}

	fmt.Println(v)

	return nil
}

func migrateHistory(c *cobra.Command, args []string) error {
	ctx := context.Background()

	drv, _, err := newMigrationDriver(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = drv.Close() }()

	err = core.MigrateHistoryDriver(ctx, drv, core.WithLockTable(migrationLockTable), core.WithLockIdentifier(lockIdentifier))
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	return nil
}

func migrateRepair(c *cobra.Command, args []string) error {
	ctx := context.Background()

	drv, _, err := newMigrationDriver(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = drv.Close() }()

	err = core.MigrateRepairDriver(ctx, drv,
		core.WithLockTable(migrationLockTable),
		core.WithLockIdentifier(lockIdentifier),
		core.WithVersionTable(migrationTableName),
	)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	return nil
}

func migrateLocker(c *cobra.Command, args []string) error {
	ctx := context.Background()

	drv, _, err := newMigrationDriver(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = drv.Close() }()

	if err := core.MigrateSetupLockDriver(ctx, drv, core.WithLockTable(migrationLockTable)); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	return nil
}

func promptDescription() string {
	fmt.Print("Please enter a short description for the migration file. Or press Enter to skip.\n>")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	clean := sanitize.Name(scanner.Text())
	if len(clean) == 1 && clean[0] == '.' { // When Enter is only pressed to skip
		return ""
	}
	return strings.ReplaceAll(clean, ".", "-") // Dot should separate .up.sql or .sql only
}
