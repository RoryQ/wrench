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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/carlmjohnson/versioninfo"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	Version         = "unknown"
	versionTemplate = `{{.Version}}
`
)

var (
	project                   string
	instance                  string
	database                  string
	directory                 string
	schemaFile                string
	credentialsFile           string
	staticDataTablesFile      string
	lockIdentifier            string
	sequenceInterval          uint16
	stmtTimeout               time.Duration
	verbose                   bool
	detectPartitionedDML      bool
	partitionedDMLConcurrency uint16
)

var rootCmd = &cobra.Command{
	Use: "wrench",
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.EnableCommandSorting = false

	rootCmd.PersistentFlags().SetNormalizeFunc(underscoreToDashes)

	rootCmd.SilenceUsage = true
	rootCmd.SilenceErrors = true

	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(dropCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(loadDiscreteCmd)
	rootCmd.AddCommand(schemaCmd)
	rootCmd.AddCommand(applyCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(truncateCmd)

	// global flags
	rootCmd.PersistentFlags().StringVar(&project, flagNameProject, spannerProjectID(), "GCP project id (optional. if not set, will use $SPANNER_PROJECT_ID or $GOOGLE_CLOUD_PROJECT value)")
	rootCmd.PersistentFlags().StringVar(&instance, flagNameInstance, spannerInstanceID(), "Cloud Spanner instance name (optional. if not set, will use $SPANNER_INSTANCE_ID value)")
	rootCmd.PersistentFlags().StringVar(&database, flagNameDatabase, spannerDatabaseID(), "Cloud Spanner database name (optional. if not set, will use $SPANNER_DATABASE_ID value)")
	rootCmd.PersistentFlags().StringVar(&directory, flagNameDirectory, "", "Directory that schema file placed (required)")
	rootCmd.PersistentFlags().StringVar(&schemaFile, flagNameSchemaFile, "", "Name of schema file (optional. if not set, will use default 'schema.sql' file name)")
	rootCmd.PersistentFlags().StringVar(&credentialsFile, flagCredentialsFile, "", "Specify Credentials File")
	rootCmd.PersistentFlags().StringVar(&staticDataTablesFile, flagStaticDataTablesFile, "", "File containing list of static data tables to track (optional)")
	rootCmd.PersistentFlags().StringVar(&lockIdentifier, flagLockIdentifier, getLockIdentifier(), "Random identifier used to lock migration operations to a single wrench process. (optional. if not set then it will be generated)")
	rootCmd.PersistentFlags().Uint16Var(&sequenceInterval, flagSequenceInterval, getSequenceInterval(), "Used to generate the next migration id. Rounds up to the next interval. (optional. if not set, will use $WRENCH_SEQUENCE_INTERVAL or default to 1)")
	rootCmd.PersistentFlags().BoolVar(&verbose, flagVerbose, false, "Used to indicate whether to output Migration information during a migration")
	rootCmd.PersistentFlags().DurationVar(&stmtTimeout, flagStmtTimeout, getStmtTimeout(), "Set a non-default timeout for statement execution")
	rootCmd.PersistentFlags().BoolVar(&detectPartitionedDML, flagDetectPartitionedDML, getDetectPartitionedDML(), "Automatically detect when a migration contains only Partitioned DML statements, and apply the statements in partition-level transactions via the PartitionedDML API. (optional. if not set, will use $WRENCH_DETECT_PARTITIONED_DML or default to false)")
	rootCmd.PersistentFlags().Uint16Var(&partitionedDMLConcurrency, flagPartitionedDMLConcurrency, getPartitionedDMLConcurrency(), "Set the concurrency for Partitioned-DML statements. (optional. if not set, will use $WRENCH_PARTITIONED_DML_CONCURRENCY or default to 1)")

	rootCmd.Version = Version
	if versioninfo.Version != "unknown" && versioninfo.Version != "(devel)" {
		rootCmd.Version = versioninfo.Version
	}
	rootCmd.SetVersionTemplate(versionTemplate)
}

func getLockIdentifier() string {
	lockID := os.Getenv("WRENCH_LOCK_IDENTIFIER")
	if lockID != "" {
		return lockID
	}
	return uuid.New().String()
}

func underscoreToDashes(f *pflag.FlagSet, name string) pflag.NormalizedName {
	return pflag.NormalizedName(strings.ReplaceAll(name, "_", "-"))
}

func spannerProjectID() string {
	projectID := os.Getenv("SPANNER_PROJECT_ID")
	if projectID != "" {
		return projectID
	}
	return os.Getenv("GOOGLE_CLOUD_PROJECT")
}

func spannerInstanceID() string {
	return os.Getenv("SPANNER_INSTANCE_ID")
}

func spannerDatabaseID() string {
	return os.Getenv("SPANNER_DATABASE_ID")
}

func getSequenceInterval() uint16 {
	i, err := strconv.Atoi(os.Getenv("WRENCH_SEQUENCE_INTERVAL"))
	if err != nil {
		return 1
	}
	return uint16(i)
}

func getStmtTimeout() time.Duration {
	i, err := time.ParseDuration(os.Getenv("WRENCH_STATEMENT_TIMEOUT"))
	if err != nil {
		return 0
	}
	return i
}

func getDetectPartitionedDML() bool {
	b, err := strconv.ParseBool(os.Getenv("WRENCH_DETECT_PARTITIONED_DML"))
	if err != nil {
		return false
	}
	return b
}

func getPartitionedDMLConcurrency() uint16 {
	i, err := strconv.Atoi(os.Getenv("WRENCH_PARTITIONED_DML_CONCURRENCY"))
	if err != nil {
		return 1
	}
	return uint16(i)
}
