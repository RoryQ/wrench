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
	"errors"
	"fmt"
	"os"

	"github.com/roryq/wrench/internal/fs"
	"github.com/roryq/wrench/pkg/core"
	"github.com/roryq/wrench/pkg/spanner"
	"github.com/spf13/cobra"
)

var (
	ddlFile     string
	dmlFile     string
	partitioned bool
)

var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply DDL file to database",
	RunE:  apply,
}

func apply(c *cobra.Command, _ []string) error {
	ctx := context.Background()

	client, err := newSpannerClient(ctx, c)
	if err != nil {
		return err
	}
	defer client.Close()

	placeholderReplacement, err := c.Flags().GetBool(flagPlaceholderReplacement)
	if err != nil {
		return &Error{
			cmd: c,
			err: err,
		}
	}

	placeholderOptions := spanner.PlaceholderOptions{
		Placeholders: core.DefaultPlaceholders(
			c.Flag(flagNameProject).Value.String(),
			c.Flag(flagNameInstance).Value.String(),
			c.Flag(flagNameDatabase).Value.String(),
		),
		ReplacementEnabled: placeholderReplacement,
	}

	if ddlFile != "" {
		if dmlFile != "" {
			return errors.New("Cannot specify DDL and DML at same time.")
		}

		ddl, err := os.ReadFile(ddlFile)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}

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

		err = client.ApplyDDLFile(ctx, ddl, placeholderOptions, protoDescriptor)
		if err != nil {
			return &Error{
				err: err,
				cmd: c,
			}
		}

		return nil
	}

	if dmlFile == "" {
		return errors.New("Must specify DDL or DML.")
	}

	// apply dml
	dml, err := os.ReadFile(dmlFile)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}

	concurrency := int(partitionedDMLConcurrency)
	numAffectedRows, err := client.ApplyDMLFile(ctx, dml, partitioned, concurrency, placeholderOptions)
	if err != nil {
		return &Error{
			err: err,
			cmd: c,
		}
	}
	fmt.Printf("%d rows affected.\n", numAffectedRows)

	return nil
}

func init() {
	applyCmd.PersistentFlags().StringVar(&ddlFile, flagDDLFile, "", "DDL file to be applied")
	applyCmd.PersistentFlags().StringVar(&dmlFile, flagDMLFile, "", "DML file to be applied")
	applyCmd.PersistentFlags().BoolVar(&partitioned, flagPartitioned, false, "Whether given DML should be executed as a Partitioned-DML or not")
	applyCmd.Flags().Bool(flagPlaceholderReplacement, true, "Enable placeholder replacement for ${PROJECT_ID}, ${INSTANCE_ID} and ${DATABASE_ID}")
	applyCmd.PersistentFlags().String(flagProtoDescriptorFile, "", "Proto descriptor file to be used with DDL operations")
}
