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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/roryq/wrench/pkg/bigquery"
	"github.com/roryq/wrench/pkg/core"
	"github.com/roryq/wrench/pkg/spanner"
)

const (
	flagNameDriver                = "driver"
	flagNameProject               = "project"
	flagNameInstance              = "instance"
	flagNameDatabase              = "database"
	flagNameDataset               = "dataset"
	flagNameLocation              = "location"
	flagNameDirectory             = "directory"
	driverSpanner                 = "spanner"
	driverBigQuery                = "bigquery"
	flagNameOutputDir             = "output-dir"
	flagSkipVersions              = "skip-versions"
	flagNameCreateNoPrompt        = "no-prompt"
	flagCredentialsFile           = "credentials-file"
	flagStaticDataTablesFile      = "static-data-tables-file"
	flagNameSchemaFile            = "schema-file"
	flagLockIdentifier            = "lock-identifier"
	flagSequenceInterval          = "sequence-interval"
	flagStmtTimeout               = "stmt-timeout"
	flagDetectPartitionedDML      = "detect-partitioned-dml"
	flagPartitionedDMLConcurrency = "partitioned-dml-concurrency"
	flagVerbose                   = "verbose"
	flagDDLFile                   = "ddl"
	flagDMLFile                   = "dml"
	flagPartitioned               = "partitioned"
	flagSpannerEmulatorImage      = "spanner-emulator-image"
	flagPlaceholderReplacement    = "placeholder-replacement"
	flagProtoDescriptorFile       = "proto-descriptor-file"
	flagFFMigrations              = "ff-migrations"
	defaultSchemaFileName         = "schema.sql"
	defaultStaticDataTablesFile   = "{wrench.json|static_data_tables.txt}"
)

func newSpannerClient(ctx context.Context, c *cobra.Command) (*spanner.Client, error) {
	if f := c.Flag(flagNameDriver); f != nil && strings.EqualFold(strings.TrimSpace(f.Value.String()), driverBigQuery) {
		return nil, &Error{
			err: fmt.Errorf("command %q is not supported for the bigquery driver", c.Name()),
			cmd: c,
		}
	}

	config := &spanner.Config{
		Project:         c.Flag(flagNameProject).Value.String(),
		Instance:        c.Flag(flagNameInstance).Value.String(),
		Database:        c.Flag(flagNameDatabase).Value.String(),
		CredentialsFile: c.Flag(flagCredentialsFile).Value.String(),
		StmtTimeout:     stmtTimeout,
	}

	client, err := spanner.NewClient(ctx, config)
	if err != nil {
		return nil, &Error{
			err: err,
			cmd: c,
		}
	}

	return client, nil
}

func resolveDriver(c *cobra.Command) (string, error) {
	d := strings.ToLower(strings.TrimSpace(c.Flag(flagNameDriver).Value.String()))
	switch d {
	case "":
		datasetChanged := c.Flags().Changed(flagNameDataset)
		instanceChanged := c.Flags().Changed(flagNameInstance)
		if datasetChanged && instanceChanged {
			return "", &Error{
				cmd: c,
				err: fmt.Errorf("both --dataset and --instance were specified; please set --driver explicitly"),
			}
		}
		if datasetChanged {
			return driverBigQuery, nil
		}
		if instanceChanged {
			return driverSpanner, nil
		}

		ds := c.Flag(flagNameDataset).Value.String()
		inst := c.Flag(flagNameInstance).Value.String()
		if ds != "" && inst == "" {
			return driverBigQuery, nil
		}
		return driverSpanner, nil
	case driverSpanner, driverBigQuery:
		return d, nil
	default:
		return "", &Error{
			cmd: c,
			err: fmt.Errorf("unsupported driver %q (must be %q or %q)", d, driverSpanner, driverBigQuery),
		}
	}
}

func resolvedBigQueryProject(c *cobra.Command) string {
	if !c.Flags().Changed(flagNameProject) {
		if bqProject := os.Getenv("BIGQUERY_PROJECT_ID"); bqProject != "" {
			return bqProject
		}
	}
	return c.Flag(flagNameProject).Value.String()
}

func newBigQueryClient(ctx context.Context, c *cobra.Command) (*bigquery.Client, error) {
	config := &bigquery.Config{
		Project:         resolvedBigQueryProject(c),
		Dataset:         c.Flag(flagNameDataset).Value.String(),
		Location:        c.Flag(flagNameLocation).Value.String(),
		CredentialsFile: c.Flag(flagCredentialsFile).Value.String(),
		StmtTimeout:     stmtTimeout,
	}

	client, err := bigquery.NewClient(ctx, config)
	if err != nil {
		return nil, &Error{
			err: err,
			cmd: c,
		}
	}

	return client, nil
}

func newMigrationDriver(ctx context.Context, c *cobra.Command) (core.Driver, string, error) {
	driverName, err := resolveDriver(c)
	if err != nil {
		return nil, "", err
	}

	switch driverName {
	case driverBigQuery:
		client, err := newBigQueryClient(ctx, c)
		if err != nil {
			return nil, "", err
		}
		return client, driverBigQuery, nil
	default:
		client, err := newSpannerClient(ctx, c)
		if err != nil {
			return nil, "", err
		}
		return core.NewSpannerDriver(client), driverSpanner, nil
	}
}

func outputDirPath(c *cobra.Command) string {
	outDir := c.Flag(flagNameOutputDir).Value.String()
	if outDir != "" {
		return outDir
	}
	return c.Flag(flagNameDirectory).Value.String()
}

func schemaFilePath(c *cobra.Command) string {
	filename := c.Flag(flagNameSchemaFile).Value.String()
	if filename == "" {
		filename = defaultSchemaFileName
	}
	return filepath.Join(c.Flag(flagNameDirectory).Value.String(), filename)
}

func staticDataTablesFilePath(c *cobra.Command) string {
	filename := c.Flag(flagStaticDataTablesFile).Value.String()
	if filename == "" {
		filename = defaultStaticDataTablesFile
	}
	return filepath.Join(c.Flag(flagNameDirectory).Value.String(), filename)
}

func protoDescriptorFilePath(c *cobra.Command) string {
	var filename string

	// Try to get the flag value from the specific command
	if flag := c.Flag(flagProtoDescriptorFile); flag != nil {
		filename = flag.Value.String()
	}

	if filename == "" {
		return ""
	}

	return filepath.Join(c.Flag(flagNameDirectory).Value.String(), filename)
}
