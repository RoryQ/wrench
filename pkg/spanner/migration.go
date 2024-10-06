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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	statementsSeparator = ";"
)

var (
	// migrationFileRegex matches the following patterns
	// 001.sql
	// 001_name.sql
	// 001_name.up.sql
	// 001_name.generated.sql
	migrationFileRegex = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?(?:[.]up|[.]generated)?\.sql$`)

	MigrationNameRegex = regexp.MustCompile(`[a-zA-Z0-9_\-]+`)

	dmlAnyRegex = regexp.MustCompile("^(UPDATE|DELETE|INSERT)[\t\n\f\r ].*")

	// 1. INSERT statements are not supported for partitioned DML. Although not every DML can be partitioned
	// as it must be idempotent. This probably isn't solvable with more regexes.
	// 2. UPDATE or DELETE statements with a SELECT statement in the WHERE clause is not fully partitionable.
	notPartitionedDmlRegex = regexp.MustCompile(`(?is)(?:insert)|(?:update|delete).*select`)

	// matches a single comment on its own line
	oneLineSingleComment = regexp.MustCompile(`(?m)^\s*--.*$`)
)

const (
	StatementKindDDL            StatementKind = "DDL"
	StatementKindDML            StatementKind = "DML"
	StatementKindPartitionedDML StatementKind = "PartitionedDML"
)

type (
	// migration represents the parsed migration file. e.g. version_name.sql
	Migration struct {
		// Version is the version of the migration
		Version uint

		// Name is the name of the migration
		Name string

		// FileName is the name of the source file for the migration
		FileName string

		// Statements is the migration statements
		Statements []string

		Kind StatementKind
	}

	Migrations []*Migration

	StatementKind string
)

func (ms Migrations) Len() int {
	return len(ms)
}

func (ms Migrations) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Migrations) Less(i, j int) bool {
	return ms[i].Version < ms[j].Version
}

func LoadMigrations(dir string, toSkipSlice []uint, detectPartitionedDML bool) (Migrations, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	toSkipMap := map[uint64]bool{}
	for _, skip := range toSkipSlice {
		toSkipMap[uint64(skip)] = true
	}

	var migrations Migrations
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		matches := migrationFileRegex.FindStringSubmatch(f.Name())
		if matches == nil {
			continue
		}

		version, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			continue
		}

		if toSkipMap[version] {
			continue
		}

		file, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			continue
		}

		statements := toStatements(file)
		kind, err := inspectStatementsKind(statements, detectPartitionedDML)
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, &Migration{
			Version:    uint(version),
			Name:       matches[2],
			FileName:   f.Name(),
			Statements: statements,
			Kind:       kind,
		})
	}

	sort.Sort(migrations)
	seen := map[uint]*Migration{}
	for _, m := range migrations {
		if dupe, got := seen[m.Version]; got {
			return nil, fmt.Errorf("migration %d %s has a duplicate version number of %s", m.Version, m.Name, dupe.Name)
		}
		seen[m.Version] = m
	}

	return migrations, nil
}

func stripComments(statement string) string {
	return oneLineSingleComment.ReplaceAllString(statement, "")
}

func stripStatement(statement string) string {
	return strings.TrimSpace(
		stripComments(
			strings.TrimSpace(
				statement)))

}

func toStatements(file []byte) []string {
	contents := bytes.Split(file, []byte(statementsSeparator))

	statements := make([]string, 0, len(contents))
	for _, c := range contents {
		if statement := stripStatement(string(c)); statement != "" {
			statements = append(statements, statement)
		}
	}

	return statements
}

func inspectStatementsKind(statements []string, detectPartitionedDML bool) (StatementKind, error) {
	kindMap := map[StatementKind]uint64{
		StatementKindDDL:            0,
		StatementKindDML:            0,
		StatementKindPartitionedDML: 0,
	}

	for _, s := range statements {
		kindMap[getStatementKind(s)]++
	}

	if distinctKind(kindMap, StatementKindDDL) {
		return StatementKindDDL, nil
	}

	// skip further DML type inspection unless detectPartitionedDML is true
	if !detectPartitionedDML && distinctKind(kindMap, StatementKindDML, StatementKindPartitionedDML) {
		return StatementKindDML, nil
	}

	if detectPartitionedDML && distinctKind(kindMap, StatementKindDML) {
		return StatementKindDML, nil
	}

	if detectPartitionedDML && distinctKind(kindMap, StatementKindPartitionedDML) {
		return StatementKindPartitionedDML, nil
	}

	return "", errors.New("Cannot specify DDL and DML in the same migration file")
}

func distinctKind(kindMap map[StatementKind]uint64, kinds ...StatementKind) bool {
	// sum the target statement kinds
	var target uint64
	for _, k := range kinds {
		target = target + kindMap[k]
	}

	// sum all statement kinds
	var total uint64
	for k := range kindMap {
		total = total + kindMap[k]
	}

	return target == total
}

func getStatementKind(statement string) StatementKind {
	if isPartitionedDMLOnly(statement) {
		return StatementKindPartitionedDML
	}

	if isDMLAny(statement) {
		return StatementKindDML
	}

	return StatementKindDDL
}

func isPartitionedDMLOnly(statement string) bool {
	return isDMLAny(statement) && !notPartitionedDmlRegex.Match([]byte(statement))
}

func isDMLAny(statement string) bool {
	return dmlAnyRegex.Match([]byte(statement))
}
