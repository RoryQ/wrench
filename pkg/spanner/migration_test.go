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
	"path/filepath"
	"testing"
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
		want      statementKind
	}{
		{
			"ALTER statement is DDL",
			TestStmtDDL,
			statementKindDDL,
		},
		{
			"UPDATE statement is PartitionedDML",
			TestStmtPartitionedDML,
			statementKindPartitionedDML,
		},
		{
			"INSERT statement is DML",
			TestStmtDML,
			statementKindDML,
		},
		{
			"lowercase insert statement is DML",
			TestStmtDML,
			statementKindDML,
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
		want                 statementKind
		detectPartitionedDML bool
		wantErr              bool
	}{
		{
			name:       "Only DDL returns DDL",
			statements: []string{TestStmtDDL, TestStmtDDL},
			want:       statementKindDDL,
		},
		{
			name:                 "Only PartitionedDML returns PartitionedDML",
			statements:           []string{TestStmtPartitionedDML, TestStmtPartitionedDML},
			want:                 statementKindPartitionedDML,
			detectPartitionedDML: true,
		},
		{
			name:                 "No PartitionedDML detection returns DML",
			statements:           []string{TestStmtPartitionedDML, TestStmtPartitionedDML},
			want:                 statementKindDML,
			detectPartitionedDML: false,
		},
		{
			name:       "Only DML returns DML",
			statements: []string{TestStmtDML, TestStmtDML},
			want:       statementKindDML,
		},
		{
			name:       "DML and DDL returns error",
			statements: []string{TestStmtDDL, TestStmtDML},
			wantErr:    true,
		},
		{
			name:       "DML and undetected PartitionedDML returns DML",
			statements: []string{TestStmtDML, TestStmtPartitionedDML},
			want:       statementKindDML,
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
			want:       statementKindDDL,
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
