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

		err = client.ApplyDDLFile(ctx, ddl)
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

	numAffectedRows, err := client.ApplyDMLFile(ctx, dml, partitioned)
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
}
