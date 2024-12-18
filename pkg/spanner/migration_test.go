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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	TestStmtDDL               = "ALTER TABLE Singers ADD COLUMN Foo STRING(MAX)"
	TestStmtPartitionedDML    = "UPDATE Singers SET FirstName = \"Bar\" WHERE SingerID = \"1\""
	TestStmtDML               = "INSERT INTO Singers(FirstName) VALUES(\"Bar\")"
	TestStmtNonPartitionedDML = "DELETE FROM Singers WHERE SingerId NOT IN (SELECT SingerId FROM Concerts)"
)

func TestLoadMigrations(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "migrations"), nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 3 {
		t.Fatalf("migrations length want 3, but got %v", len(ms))
	}

	testcases := []struct {
		idx         int
		wantVersion uint
		wantName    string
	}{
		{
			idx:         0,
			wantVersion: 2,
			wantName:    "test",
		},
		{
			idx:         1,
			wantVersion: 3,
			wantName:    "",
		},
	}

	for _, tc := range testcases {
		if ms[tc.idx].Version != tc.wantVersion {
			t.Errorf("migrations[%d].version want %v, but got %v", tc.idx, tc.wantVersion, ms[tc.idx].Version)
		}

		if ms[tc.idx].Name != tc.wantName {
			t.Errorf("migrations[%d].name want %v, but got %v", tc.idx, tc.wantName, ms[tc.idx].Name)
		}
	}
}

func TestLoadMigrationsSkipVersion(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "migrations"), []uint{2, 3}, false)
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 1 {
		t.Fatalf("migrations length want 1, but got %v", len(ms))
	}

	if ms[0].Version != 4 {
		t.Errorf("version want %v, but got %v", 4, ms[0].Version)
	}
}

func TestLoadMigrationsDuplicates(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "duplicate"), nil, false)
	if err == nil {
		t.Errorf("error should not be nil")
	}
	if len(ms) > 0 {
		t.Errorf("migrations should be empty")
	}
}

func Test_getStatementKind(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      StatementKind
	}{
		{
			"ALTER statement is DDL",
			TestStmtDDL,
			StatementKindDDL,
		},
		{
			"UPDATE statement is PartitionedDML",
			TestStmtPartitionedDML,
			StatementKindPartitionedDML,
		},
		{
			"INSERT statement is DML",
			TestStmtDML,
			StatementKindDML,
		},
		{
			"lowercase insert statement is DML",
			TestStmtDML,
			StatementKindDML,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStatementKind(tt.statement); got != tt.want {
				t.Errorf("getStatementKind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inspectStatementsKind(t *testing.T) {
	tests := []struct {
		name                 string
		statements           []string
		want                 StatementKind
		detectPartitionedDML bool
		wantErr              bool
	}{
		{
			name:       "Only DDL returns DDL",
			statements: []string{TestStmtDDL, TestStmtDDL},
			want:       StatementKindDDL,
		},
		{
			name:                 "Only PartitionedDML returns PartitionedDML",
			statements:           []string{TestStmtPartitionedDML, TestStmtPartitionedDML},
			want:                 StatementKindPartitionedDML,
			detectPartitionedDML: true,
		},
		{
			name:                 "No PartitionedDML detection returns DML",
			statements:           []string{TestStmtPartitionedDML, TestStmtPartitionedDML},
			want:                 StatementKindDML,
			detectPartitionedDML: false,
		},
		{
			name:       "Only DML returns DML",
			statements: []string{TestStmtDML, TestStmtDML},
			want:       StatementKindDML,
		},
		{
			name:       "DML and DDL returns error",
			statements: []string{TestStmtDDL, TestStmtDML},
			wantErr:    true,
		},
		{
			name:       "DML and undetected PartitionedDML returns DML",
			statements: []string{TestStmtDML, TestStmtPartitionedDML},
			want:       StatementKindDML,
		},
		{
			name:                 "DML and detected PartitionedDML returns error",
			statements:           []string{TestStmtDML, TestStmtPartitionedDML},
			wantErr:              true,
			detectPartitionedDML: true,
		},
		{
			name:       "DDL and undetected PartitionedDML returns error",
			statements: []string{TestStmtDDL, TestStmtPartitionedDML},
			wantErr:    true,
		},
		{
			name:                 "DDL and detected PartitionedDML returns error",
			statements:           []string{TestStmtDDL, TestStmtPartitionedDML},
			wantErr:              true,
			detectPartitionedDML: true,
		},
		{
			name:       "no statements defaults to DDL as before",
			statements: []string{},
			want:       StatementKindDDL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inspectStatementsKind(tt.statements, tt.detectPartitionedDML)
			if (err != nil) != tt.wantErr {
				t.Errorf("inspectStatementsKind() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("inspectStatementsKind() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stripStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name: "Basic single line comment is removed",
			statement: `-- THIS SHOULD BE REMOVED
CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID)`,
			want: `CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID)`,
		},
		{
			name: "Maligned single line comment is removed",
			statement: `  -- THIS SHOULD BE REMOVED
CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID)`,
			want: `CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID)`,
		},
		//		{
		//			name: "Single line comment after SQL is removed",
		//			statement: `CREATE TABLE SchemaMigrations (
		//  Version INT64 NOT NULL,
		//  Dirty BOOL NOT NULL, -- THIS SHOULD BE REMOVED
		// ) PRIMARY KEY(Version)`,
		//			want: `CREATE TABLE SchemaMigrations (
		//  Version INT64 NOT NULL,
		//  Dirty BOOL NOT NULL,
		// ) PRIMARY KEY(Version)`,
		//		},
		//		{
		//			name: "Double quoted comment remains",
		//			statement: `INSERT INTO Singers(SingerID, FirstName) VALUES(1, "first name
		// -- THIS STAYS IN DOUBLE QUOTES
		// ")`,
		//			want: `INSERT INTO Singers(SingerID, FirstName) VALUES(1, "first name
		// -- THIS STAYS IN DOUBLE QUOTES
		// ")`,
		//		},
		//		{
		//			name: "Single quoted comment remains",
		//			statement: `INSERT INTO Singers(SingerID, FirstName) VALUES(1, 'first name
		// -- THIS STAYS IN DOUBLE QUOTES
		// ')`,
		//			want: `INSERT INTO Singers(SingerID, FirstName) VALUES(1, 'first name
		// -- THIS STAYS IN DOUBLE QUOTES
		// ')`,
		//		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripStatement(tt.statement); got != tt.want {
				t.Errorf("stripStatement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isPartitionedDMLOnly(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      bool
	}{
		{
			"ALTER statement is DDL",
			TestStmtDDL,
			false,
		},
		{
			"UPDATE statement is PartitionedDML",
			TestStmtPartitionedDML,
			true,
		},
		{
			"INSERT statement is not prtitioned DML",
			TestStmtDML,
			false,
		},
		{
			"DELETE without SELECT is partitioned DML",
			`DELETE FROM Singers WHERE SingerId = 123`,
			true,
		},
		{
			"DELETE statment with SELECT is not fully partitioned DML",
			TestStmtNonPartitionedDML,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPartitionedDMLOnly(tt.statement); got != tt.want {
				t.Errorf("isPartitionedDMLOnly() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_migrationFileRegex(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected []string
	}{
		"NoName": {
			input:    "001.sql",
			expected: []string{"001.sql", "001", ""},
		},
		"WithName": {
			input:    "001_name.sql",
			expected: []string{"001_name.sql", "001", "name"},
		},
		"MatchAndIgnoreUp": {
			input:    "001_name.up.sql",
			expected: []string{"001_name.up.sql", "001", "name"},
		},
		"MatchAndIgnoreGenerated": {
			input:    "001_name.generated.sql",
			expected: []string{"001_name.generated.sql", "001", "name"},
		},
		"NotMatchDownMigration": {
			input:    "001_name.down.sql",
			expected: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			matches := migrationFileRegex.FindStringSubmatch(tc.input)
			assert.Equal(t, tc.expected, matches)
		})
	}
}

func Test_parseMigrationDirectives(t *testing.T) {
	tests := []struct {
		name string
		data string
		want MigrationDirectives
	}{
		{
			name: "NoPreamble",
			data: "SELECT 1 FROM Foo",
			want: MigrationDirectives{},
		},
		{
			name: "PreambleWithoutDirectives",
			data: `
/*
 this is my
 preamble
*/
SELECT 1 FROM Foo`,
			want: MigrationDirectives{},
		},
		{
			name: "PreambleWithDirectives_BlockComment",
			data: fmt.Sprintf(`
/*
 @wrench.migrationKind=%s
 @wrench.concurrency=123
*/
SELECT 1 FROM Foo`, MigrationKindFixedPointIterationDML),
			want: MigrationDirectives{
				MigrationKind: MigrationKindFixedPointIterationDML,
				Concurrency:   123,
			},
		},
		{
			name: "PreambleWithDirectives_LineComment",
			data: fmt.Sprintf(`
-- @wrench.migrationKind=%s
-- @wrench.concurrency=123
SELECT 1 FROM Foo`, MigrationKindFixedPointIterationDML),
			want: MigrationDirectives{
				MigrationKind: MigrationKindFixedPointIterationDML,
				Concurrency:   123,
			},
		},
		{
			name: "PreambleWithDirectives_DirectiveCommentIgnored",
			data: fmt.Sprintf(`
/*
 @wrench.migrationKind=%s // This is ignored
*/
SELECT 1 FROM Foo
`, MigrationKindFixedPointIterationDML),
			want: MigrationDirectives{
				MigrationKind: MigrationKindFixedPointIterationDML,
			},
		},
		{
			name: "WhitespaceIgnored",
			data: fmt.Sprintf(`
/*         @wrench.migrationKind=%s           */
--         @wrench.concurrency=123           
SELECT 1 FROM Foo
`, MigrationKindFixedPointIterationDML),
			want: MigrationDirectives{
				MigrationKind: MigrationKindFixedPointIterationDML,
				Concurrency:   123,
			},
		},
		{
			name: "NonDirectivesIgnored",
			data: fmt.Sprintf(`
/*
This is my migration!

@wrench.migrationKind=%s

Foo bar baz.

@wrench.concurrency=123
*/
SELECT 1 FROM Foo
`, MigrationKindFixedPointIterationDML),
			want: MigrationDirectives{
				MigrationKind: MigrationKindFixedPointIterationDML,
				Concurrency:   123,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMigrationDirectives(tt.data)
			assert.Equal(t, tt.want, got)
			assert.NoError(t, err)
		})
	}

	t.Run("Errors", func(t *testing.T) {
		t.Run("InvalidConcurrency", func(t *testing.T) {

			got, err := parseMigrationDirectives(`/*
@wrench.migrationKind=%s
@wrench.concurrency=abc
*/
SELECT 1 FROM Foo
`)
			assert.Zero(t, got)
			assert.Error(t, err)
		})
	})
}

func Test_extractPreamble(t *testing.T) {
	tests := []struct {
		name string
		data string
		want []string
	}{
		{
			name: "NoPreamble",
			data: "SELECT 1 FROM Foo",
			want: nil,
		},
		{
			name: "BlockPreamble",
			data: `
/*
 this is my
 preamble
*/
SELECT 1 FROM Foo`,
			want: []string{"this is my", "preamble"},
		},
		{
			name: "InlineBlockDelimiters",
			data: `
/* this is my
 preamble */
SELECT 1 FROM Foo`,
			want: []string{"this is my", "preamble"},
		},
		{
			name: "SingleLineBlockComment",
			data: `
/* this is my preamble */
SELECT 1 FROM Foo`,
			want: []string{"this is my preamble"},
		},
		{
			name: "LineCommentPreamble",
			data: `
-- this is my
-- preamble
SELECT 1 FROM Foo`,
			want: []string{"this is my", "preamble"},
		},
		{
			name: "IgnoresNonPreambleLineComments",
			data: `
-- this is my
-- preamble
SELECT 1 FROM Foo -- this is not preamble
 -- this is also not preamble`,
			want: []string{"this is my", "preamble"},
		},
		{
			name: "IgnoresNonPreambleBlockComments",
			data: `
/* this is my preamble */
SELECT 1 FROM Foo /* this is not preamble */
/* this is not preamble */`,
			want: []string{"this is my preamble"},
		},
		{
			name: "ExtractsPreambleAcrossMultipleBlocks",
			data: `
/* 
 block 1
*/
-- block 2
/*block 3*/
SELECT 1 FROM Foo
/* this is not preamble */`,
			want: []string{"block 1", "block 2", "block 3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractPreamble(tt.data))
		})
	}
}
