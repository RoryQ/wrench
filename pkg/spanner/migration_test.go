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

var (
	TestPlaceholders = map[string]string{
		"PROJECT_ID":  "projectID134",
		"INSTANCE_ID": "instanceID456",
		"DATABASE_ID": "databaseID789",
	}
)

func TestLoadMigrations(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "migrations"), nil, false, PlaceholderOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 4 {
		t.Fatalf("migrations length want 4, but got %v", len(ms))
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
	ms, err := LoadMigrations(filepath.Join("testdata", "migrations"), []uint{2, 3, 4}, false, PlaceholderOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 1 {
		t.Fatalf("migrations length want 1, but got %v", len(ms))
	}

	if ms[0].Version != 5 {
		t.Errorf("version want %v, but got %v", 5, ms[0].Version)
	}
}

func TestLoadMigrationsDuplicates(t *testing.T) {
	ms, err := LoadMigrations(filepath.Join("testdata", "duplicate"), nil, false, PlaceholderOptions{})
	if err == nil {
		t.Errorf("error should not be nil")
	}
	if len(ms) > 0 {
		t.Errorf("migrations should be empty")
	}
}

func TestLoadMigrationsSubstitutePlaceHolders(t *testing.T) {
	placeholderOptions := PlaceholderOptions{
		Placeholders:       TestPlaceholders,
		ReplacementEnabled: true,
	}
	ms, err := LoadMigrations(filepath.Join("testdata", "placeholders"), nil, false, placeholderOptions)
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 1 {
		t.Fatalf("migrations length want 1, but got %v", len(ms))
	}
}

func TestReplacePlaceholders(t *testing.T) {
	tests := []struct {
		name         string
		statements   []string
		placeholders map[string]string
		want         []string
		wantErr      bool
	}{
		{
			name: "successfully replaces all placeholders with values",

			statements: []string{
				`UPDATE Singers SET FirstName = "${PROJECT_ID}" WHERE SingerID = "${DATABASE_ID}";`,
				`UPDATE Singers SET FirstName = "${INSTANCE_ID}" WHERE SingerID = "${DATABASE_ID}";`,
			},
			placeholders: TestPlaceholders,
			want: []string{
				`UPDATE Singers SET FirstName = "projectID134" WHERE SingerID = "databaseID789";`,
				`UPDATE Singers SET FirstName = "instanceID456" WHERE SingerID = "databaseID789";`,
			},
			wantErr: false,
		},
		{
			name: "does not replace placeholders with spaces",
			statements: []string{
				`UPDATE Singers SET FirstName = "${ PROJECT_ID }" WHERE SingerID = "${ DATABASE_ID }";`,
				`UPDATE Singers SET FirstName = "${ INSTANCE_ID }" WHERE SingerID = "${ DATABASE_ID }";`,
			},
			placeholders: TestPlaceholders,
			want: []string{
				`UPDATE Singers SET FirstName = "${ PROJECT_ID }" WHERE SingerID = "${ DATABASE_ID }";`,
				`UPDATE Singers SET FirstName = "${ INSTANCE_ID }" WHERE SingerID = "${ DATABASE_ID }";`,
			},
			wantErr: false,
		},
		{
			name: "errors when placeholders do not match case",
			statements: []string{
				`UPDATE Singers SET FirstName = "${PrOjECT_iD}" WHERE SingerID = "${dataBase_iD}";`,
				`UPDATE Singers SET FirstName = "${InsTanCe_iD}" WHERE SingerID = "${dataBase_iD}";`,
			},
			placeholders: TestPlaceholders,
			wantErr:      true,
		},
		{
			name: "errors when one statement refers to a placeholder that is not configured",
			statements: []string{
				`UPDATE Singers SET FirstName = "${PROJECT_ID}" WHERE SingerID = "${DATABASE_ID}";`,
				`UPDATE Singers SET FirstName = "${VERSION}" WHERE SingerID = "${DATABASE_ID}";`,
			},
			placeholders: TestPlaceholders,

			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := replacePlaceholders(tt.statements, tt.placeholders)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.ElementsMatch(t, tt.want, got)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("replacePlaceholders() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
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

func TestRemoveCommentsAndTrim(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: ``,
			want:  ``,
		},
		{
			input: `SELECT 1;`,
			want:  `SELECT 1`,
		},
		{
			input: `-- This is a single line comment
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `# This is a single line comment
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This is a multi line comment on one line */
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This
is
a
multiline
comment
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/* This
* is
* a
* multiline
* comment
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment on one line */
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This
is
a
javadoc
style
comment
on
multiple
lines
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `/** This
* is
* a
* javadoc
* style
* comment
* on
* multiple
* lines
*/
SELECT 1;`,
			want: `SELECT 1`,
		},
		{
			input: `-- First comment
SELECT--second comment
1`,
			want: `SELECT
1`,
		},
		{
			input: `# First comment
SELECT#second comment
1`,
			want: `SELECT
1`,
		},
		{
			input: `-- First comment
SELECT--second comment
1--third comment`,
			want: `SELECT
1`,
		},
		{
			input: `# First comment
SELECT#second comment
1#third comment`,
			want: `SELECT
1`,
		},
		{
			input: `/* First comment */
SELECT/* second comment */
1`,
			want: `SELECT
1`,
		},
		{
			input: `/* First comment */
SELECT/* second comment */
1/* third comment */`,
			want: `SELECT
1`,
		},
		{
			input: `SELECT "TEST -- This is not a comment"`,
			want:  `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `-- This is a comment
SELECT "TEST -- This is not a comment"`,
			want: `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `-- This is a comment
SELECT "TEST -- This is not a comment" -- This is a comment`,
			want: `SELECT "TEST -- This is not a comment"`,
		},
		{
			input: `SELECT "TEST # This is not a comment"`,
			want:  `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `# This is a comment
SELECT "TEST # This is not a comment"`,
			want: `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `# This is a comment
SELECT "TEST # This is not a comment" # This is a comment`,
			want: `SELECT "TEST # This is not a comment"`,
		},
		{
			input: `SELECT "TEST /* This is not a comment */"`,
			want:  `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `/* This is a comment */
SELECT "TEST /* This is not a comment */"`,
			want: `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `/* This is a comment */
SELECT "TEST /* This is not a comment */" /* This is a comment */`,
			want: `SELECT "TEST /* This is not a comment */"`,
		},
		{
			input: `SELECT 'TEST -- This is not a comment'`,
			want:  `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `-- This is a comment
SELECT 'TEST -- This is not a comment'`,
			want: `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `-- This is a comment
SELECT 'TEST -- This is not a comment' -- This is a comment`,
			want: `SELECT 'TEST -- This is not a comment'`,
		},
		{
			input: `SELECT 'TEST # This is not a comment'`,
			want:  `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `# This is a comment
SELECT 'TEST # This is not a comment'`,
			want: `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `# This is a comment
SELECT 'TEST # This is not a comment' # This is a comment`,
			want: `SELECT 'TEST # This is not a comment'`,
		},
		{
			input: `SELECT 'TEST /* This is not a comment */'`,
			want:  `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `/* This is a comment */
SELECT 'TEST /* This is not a comment */'`,
			want: `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `/* This is a comment */
SELECT 'TEST /* This is not a comment */' /* This is a comment */`,
			want: `SELECT 'TEST /* This is not a comment */'`,
		},
		{
			input: `SELECT '''TEST
-- This is not a comment
'''`,
			want: `SELECT '''TEST
-- This is not a comment
'''`,
		},
		{
			input: ` -- This is a comment
SELECT '''TEST
-- This is not a comment
''' -- This is a comment`,
			want: `SELECT '''TEST
-- This is not a comment
'''`,
		},
		{
			input: `SELECT '''TEST
# This is not a comment
'''`,
			want: `SELECT '''TEST
# This is not a comment
'''`,
		},
		{
			input: ` # This is a comment
SELECT '''TEST
# This is not a comment
''' # This is a comment`,
			want: `SELECT '''TEST
# This is not a comment
'''`,
		},
		{
			input: `SELECT '''TEST
/* This is not a comment */
'''`,
			want: `SELECT '''TEST
/* This is not a comment */
'''`,
		},
		{
			input: ` /* This is a comment */
SELECT '''TEST
/* This is not a comment */
''' /* This is a comment */`,
			want: `SELECT '''TEST
/* This is not a comment */
'''`,
		},
		{
			input: `SELECT """TEST
-- This is not a comment
"""`,
			want: `SELECT """TEST
-- This is not a comment
"""`,
		},
		{
			input: ` -- This is a comment
SELECT """TEST
-- This is not a comment
""" -- This is a comment`,
			want: `SELECT """TEST
-- This is not a comment
"""`,
		},
		{
			input: `SELECT """TEST
# This is not a comment
"""`,
			want: `SELECT """TEST
# This is not a comment
"""`,
		},
		{
			input: ` # This is a comment
SELECT """TEST
# This is not a comment
""" # This is a comment`,
			want: `SELECT """TEST
# This is not a comment
"""`,
		},
		{
			input: `SELECT """TEST
/* This is not a comment */
"""`,
			want: `SELECT """TEST
/* This is not a comment */
"""`,
		},
		{
			input: ` /* This is a comment */
SELECT """TEST
/* This is not a comment */
""" /* This is a comment */`,
			want: `SELECT """TEST
/* This is not a comment */
"""`,
		},
		{
			input: `/* This is a comment /* this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /* this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /** this is still a comment */
SELECT 1`,
			want: `SELECT 1`,
		},
		{
			input: `/** This is a javadoc style comment /** this is still a comment **/
SELECT 1`,
			want: `SELECT 1`,
		},
	}
	for _, tc := range tests {
		got, err := removeCommentsAndTrim(tc.input)
		if err != nil && !tc.wantErr {
			t.Error(err)
			continue
		}
		if tc.wantErr {
			t.Errorf("missing expected error for %q", tc.input)
			continue
		}
		if got != tc.want {
			t.Errorf("removeCommentsAndTrim result mismatch\nGot: %q\nWant: %q", got, tc.want)
		}
	}
}

func Test_parseMigrationDirectives(t *testing.T) {
	const testStatementKind = StatementKind("Foo")

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
 @wrench.StatementKind=%s
 @wrench.Concurrency=123
*/
SELECT 1 FROM Foo`, testStatementKind),
			want: MigrationDirectives{
				StatementKind: testStatementKind,
				Concurrency:   123,
			},
		},
		{
			name: "PreambleWithDirectives_LineComment",
			data: fmt.Sprintf(`
-- @wrench.StatementKind=%s
-- @wrench.Concurrency=123
SELECT 1 FROM Foo`, testStatementKind),
			want: MigrationDirectives{
				StatementKind: testStatementKind,
				Concurrency:   123,
			},
		},
		{
			name: "PreambleWithDirectives_DirectiveCommentIgnored",
			data: fmt.Sprintf(`
/*
 @wrench.StatementKind=%s // This is ignored
*/
SELECT 1 FROM Foo
`, testStatementKind),
			want: MigrationDirectives{
				StatementKind: testStatementKind,
			},
		},
		{
			name: "WhitespaceIgnored",
			data: fmt.Sprintf(`
/*         @wrench.StatementKind=%s           */
--         @wrench.Concurrency=123           
SELECT 1 FROM Foo
`, testStatementKind),
			want: MigrationDirectives{
				StatementKind: testStatementKind,
				Concurrency:   123,
			},
		},
		{
			name: "NonDirectivesIgnored",
			data: fmt.Sprintf(`
/*
This is my migration!

@wrench.StatementKind=%s

Foo bar baz.

@wrench.Concurrency=123
*/
SELECT 1 FROM Foo
`, testStatementKind),
			want: MigrationDirectives{
				StatementKind: testStatementKind,
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
			got, err := parseMigrationDirectives(fmt.Sprintf(`/*
@wrench.StatementKind=%s
@wrench.Concurrency=abc
*/
SELECT 1 FROM Foo
`, testStatementKind))
			assert.Zero(t, got)
			assert.Error(t, err)
		})

		t.Run("UnknownKey", func(t *testing.T) {
			got, err := parseMigrationDirectives(`
-- @wrench.foo=bar
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
		want string
	}{
		{
			name: "NoPreamble",
			data: "SELECT 1 FROM Foo",
			want: "",
		},
		{
			name: "BlockPreamble",
			data: `
/*
 this is my
 preamble
*/
SELECT 1 FROM Foo`,
			want: `this is my
preamble`,
		},
		{
			name: "InlineBlockDelimiters",
			data: `
/* this is my
 preamble */
SELECT 1 FROM Foo`,
			want: `this is my
preamble`,
		},
		{
			name: "SingleLineBlockComment",
			data: `
/* this is my preamble */
SELECT 1 FROM Foo`,
			want: `this is my preamble`,
		},
		{
			name: "LineCommentPreamble",
			data: `
-- this is my
-- preamble
SELECT 1 FROM Foo`,
			want: `this is my
preamble`,
		},
		{
			name: "HashLineCommentPreamble",
			data: `
# this is my
# preamble
SELECT 1 FROM Foo`,
			want: `this is my
preamble`,
		},
		{
			name: "IgnoresNonPreambleLineComments",
			data: `
-- this is my
-- preamble
SELECT 1 FROM Foo -- this is not preamble
 -- this is also not preamble`,
			want: `this is my
preamble`,
		},
		{
			name: "IgnoresNonPreambleBlockComments",
			data: `
/* this is my preamble */
SELECT 1 FROM Foo /* this is not preamble */
/* this is not preamble */`,
			want: `this is my preamble`,
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
			want: `block 1
block 2
block 3`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, extractPreamble(tt.data))
		})
	}
}
