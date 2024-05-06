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
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/kennygrant/sanitize"
	"github.com/spf13/cobra"

	"github.com/roryq/wrench/pkg/core"
	"github.com/roryq/wrench/pkg/spanner"
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

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	migrationsDir := filepath.Join(c.Flag(flagNameDirectory).Value.String(), migrationsDirName)
	err = core.MigrateUp(ctx, client, migrationsDir,
		core.WithLimit(limit),
		core.WithSkipVersions(toSkip),
		core.WithLockIdentifier(lockIdentifier),
		core.WithVersionTable(migrationTableName),
		core.WithLockTable(migrationLockTable),
		core.WithPartitionedDMLConcurrency(partitionedDMLConcurrency),
		core.WithDetectPartitionedDML(detectPartitionedDML),
		core.WithPrintRowsAffected(verbose),
	)
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

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	v, _, err := client.GetSchemaMigrationVersion(ctx, migrationTableName)
	if err != nil {
		var se *spanner.Error
		if errors.As(err, &se) && se.Code == spanner.ErrorCodeNoMigration {
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

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	lock, err := client.GetMigrationLock(ctx, migrationLockTable, lockIdentifier)
	defer lock.Release()
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	if !lock.Success {
		return &Error{
			cmd: c,
			err: fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry),
		}
	}

	history, err := client.GetMigrationHistory(ctx, migrationTableName)
	if err != nil {
		return err
	}
	sort.SliceStable(history, func(i, j int) bool {
		return history[i].Created.Before(history[j].Created) // order by Created
	})

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
	fmt.Fprintln(writer, "Version\tDirty\tCreated\tModified")
	for i := range history {
		h := history[i]
		fmt.Fprintf(writer, "%d\t%v\t%v\t%v\n", h.Version, h.Dirty, h.Created, h.Modified)
	}
	writer.Flush()

	return nil
}

func migrateRepair(c *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()
	lock, err := client.GetMigrationLock(ctx, migrationLockTable, lockIdentifier)
	defer lock.Release()
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}
	if !lock.Success {
		return &Error{
			cmd: c,
			err: fmt.Errorf("lock taken by another process %s which expires %v", lock.LockIdentifier, lock.Expiry),
		}
	}

	if err = client.EnsureMigrationTable(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	if err := client.RepairMigration(ctx, migrationTableName); err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	return nil
}

func migrateLocker(c *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := client.SetupMigrationLock(ctx, migrationLockTable); err != nil {
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
