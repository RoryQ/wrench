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

package spanner

import (
	mig "github.com/roryq/wrench/pkg/migration"
)

var (
	MigrationNameRegex       = mig.MigrationNameRegex
	migrationFileRegex       = mig.MigrationFileRegex
	repeatableMigrationRegex = mig.RepeatableMigrationRegex
)

const (
	StatementKindDDL            = mig.StatementKindDDL
	StatementKindDML            = mig.StatementKindDML
	StatementKindPartitionedDML = mig.StatementKindPartitionedDML
	StatementKindConvergentDML  = mig.StatementKindConvergentDML
)

type (
	Migration           = mig.Migration
	MigrationDirectives = mig.MigrationDirectives
	Migrations          = mig.Migrations
	StatementKind       = mig.StatementKind
	PlaceholderOptions  = mig.PlaceholderOptions
)

func LoadMigrations(dir string, toSkipSlice []uint, detectPartitionedDML bool, placeholderOptions PlaceholderOptions) (Migrations, error) {
	return mig.LoadMigrations(dir, toSkipSlice, detectPartitionedDML, placeholderOptions)
}

func toStatements(file []byte) ([]string, error) {
	return mig.ToStatements(file)
}

func replacePlaceholders(statements []string, placeholders map[string]string) ([]string, error) {
	return mig.ReplacePlaceholders(statements, placeholders)
}

func inspectStatementsKind(statements []string, detectPartitionedDML bool) (StatementKind, error) {
	return mig.InspectStatementsKind(statements, detectPartitionedDML)
}

func getStatementKind(statement string) StatementKind {
	return mig.GetStatementKind(statement)
}

func isPartitionedDMLOnly(statement string) bool {
	return mig.IsPartitionedDMLOnly(statement)
}

func removeCommentsAndTrim(sql string) (string, error) {
	return mig.RemoveCommentsAndTrim(sql)
}

func parseMigrationDirectives(m string) (MigrationDirectives, error) {
	return mig.ParseMigrationDirectives(m)
}

func extractPreamble(m string) string {
	return mig.ExtractPreamble(m)
}
