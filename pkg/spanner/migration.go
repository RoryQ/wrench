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

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
  
  "github.com/roryq/wrench/pkg/xregexp"
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

		// Directives defines config scoped to a single migration.
		Directives MigrationDirectives
	}

	// MigrationDirectives configures how the migration should be executed.
	MigrationDirectives struct {
		placeholder string
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

		statements, err := toStatements(file)
		if err != nil {
			return nil, err
		}

		kind, err := inspectStatementsKind(statements, detectPartitionedDML)
		if err != nil {
			return nil, err
		}

		// Parse any migration-scoped directives for the migration
		directives, err := parseMigrationDirectives(string(file))
		if err != nil {
			return nil, err
		}

		migrations = append(migrations, &Migration{
			Version:    uint(version),
			Name:       matches[2],
			FileName:   f.Name(),
			Statements: statements,
			Kind:       kind,
			Directives: directives,
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

func toStatements(file []byte) ([]string, error) {
	contents := bytes.Split(file, []byte(statementsSeparator))

	statements := make([]string, 0, len(contents))
	for _, c := range contents {
		statement, err := removeCommentsAndTrim(string(c))
		if err != nil {
			return nil, err
		}
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements, nil
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

// RemoveCommentsAndTrim removes any comments in the query string and trims any
// spaces at the beginning and end of the query. This makes checking what type
// of query a string is a lot easier, as only the first word(s) need to be
// checked after this has been removed.
//
// This function is lifted directly from googleapis/go-sql-spanner.
// https://github.com/googleapis/go-sql-spanner/blob/076c63111370017133f79dd37c0069f68f27d7df/statement_parser.go
func removeCommentsAndTrim(sql string) (string, error) {
	const singleQuote = '\''
	const doubleQuote = '"'
	const backtick = '`'
	const hyphen = '-'
	const dash = '#'
	const slash = '/'
	const asterisk = '*'
	isInQuoted := false
	isInSingleLineComment := false
	isInMultiLineComment := false
	var startQuote rune
	lastCharWasEscapeChar := false
	isTripleQuoted := false
	res := strings.Builder{}
	res.Grow(len(sql))
	index := 0
	runes := []rune(sql)
	for index < len(runes) {
		c := runes[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return "", spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", sql))
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
						res.WriteRune(c)
						res.WriteRune(c)
						index += 2
					}
				} else {
					isInQuoted = false
					startQuote = 0
				}
			} else if c == '\\' {
				lastCharWasEscapeChar = true
			} else {
				lastCharWasEscapeChar = false
			}
			res.WriteRune(c)
		} else {
			// We are not in a quoted string.
			if isInSingleLineComment {
				if c == '\n' {
					isInSingleLineComment = false
					// Include the line feed in the result.
					res.WriteRune(c)
				}
			} else if isInMultiLineComment {
				if len(runes) > index+1 && c == asterisk && runes[index+1] == slash {
					isInMultiLineComment = false
					index++
				}
			} else {
				if c == dash || (len(runes) > index+1 && c == hyphen && runes[index+1] == hyphen) {
					// This is a single line comment.
					isInSingleLineComment = true
				} else if len(runes) > index+1 && c == slash && runes[index+1] == asterisk {
					isInMultiLineComment = true
					index++
				} else {
					if c == singleQuote || c == doubleQuote || c == backtick {
						isInQuoted = true
						startQuote = c
						// Check whether it is a triple-quote.
						if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
							isTripleQuoted = true
							res.WriteRune(c)
							res.WriteRune(c)
							index += 2
						}
					}
					res.WriteRune(c)
				}
			}
		}
		index++
	}
	if isInQuoted {
		return "", spanner.ToSpannerError(status.Errorf(codes.InvalidArgument, "statement contains an unclosed literal: %s", sql))
	}
	trimmed := strings.TrimSpace(res.String())
	if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';' {
		return trimmed[:len(trimmed)-1], nil
	}
	return trimmed, nil
}

// parseMigrationDirectives extracts migration directives in the format
// @wrench.{key}={value} from the migration preamble.
func parseMigrationDirectives(migration string) (MigrationDirectives, error) {
	const (
		// placeholderKey is a placeholder to validate parsing until a directive
		// is implemented.
		placeholderKey = "TODO"
	)

	// matches a migration directive in the format @wrench.{key}={value}
	directiveRegex := regexp.MustCompile(`(?m)^\s*@wrench[.](?P<Key>\w+)=(?P<Value>\w+)`)
	directiveMatches, _ := xregexp.FindAllMatchGroups(directiveRegex, extractPreamble(migration))

	var directives MigrationDirectives
	for _, match := range directiveMatches {
		key, val := match["Key"], match["Value"]
		switch key {
		case placeholderKey:
			directives.placeholder = val
		default:
			return directives, fmt.Errorf("unknown migration directive: %s", key)
		}
	}

	return directives, nil
}

// extractPreamble returns all comments from the start of a migration file,
// until the first non-empty non-comment line is encountered.
func extractPreamble(migration string) string {
	const (
		blockCommentStart    = "/*"
		blockCommentEnd      = "*/"
		lineCommentPrefix    = "--"
		lineCommentAltPrefix = "#"
	)

	var comments []string
	var blockComment bool
	for _, line := range strings.Split(migration, "\n") {
		// Skip empty lines.
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Look for block or line comment start.
		if !blockComment {
			if strings.HasPrefix(line, blockCommentStart) {
				blockComment = true
				_, line, _ = strings.Cut(line, blockCommentStart)
			} else if strings.HasPrefix(line, lineCommentPrefix) {
				line = strings.TrimPrefix(line, lineCommentPrefix)
			} else if strings.HasPrefix(line, lineCommentAltPrefix) {
				line = strings.TrimPrefix(line, lineCommentAltPrefix)
			} else {
				// Not in a block comment or line comment, and the line is not
				// empty. Preamble is over.
				break
			}
		}

		// Look for block comment exit.
		if blockComment && strings.Contains(line, blockCommentEnd) {
			line, _, _ = strings.Cut(line, blockCommentEnd)
			blockComment = false
		}

		// Capture non-empty comment lines
		if line = strings.TrimSpace(line); line != "" {
			comments = append(comments, line)
		}
	}
  
	return strings.Join(comments, "\n")
}