package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/roryq/wrench/pkg/spanner"
)

func Test_readStaticDataTablesFile(t *testing.T) {
	type args struct {
		filePath string
	}
	JsonTables := staticDataConfig{StaticDataTables: []string{"JsonTable1", "JsonTable2"}, CustomOrderBy: map[string]string{"JsonTable1": "Column1 ASC, Column2 DESC"}}
	TxtTables := staticDataConfig{StaticDataTables: []string{"TxtTable1", "TxtTable2"}}
	tests := []struct {
		name    string
		args    args
		wantSdc staticDataConfig
		wantErr bool
	}{
		{
			name: "default uses json first",
			args: args{
				filePath: "testdata/{wrench.json|static_data_tables.txt}",
			},
			wantSdc: JsonTables,
		},
		{
			name: "default uses txt second",
			args: args{
				filePath: "testdata/txt/{wrench.json|static_data_tables.txt}",
			},
			wantSdc: TxtTables,
		},
		{
			name: "specify json file",
			args: args{
				filePath: "testdata/wrench.json",
			},
			wantSdc: JsonTables,
		},
		{
			name: "specify txt file",
			args: args{
				filePath: "testdata/static_data_tables.txt",
			},
			wantSdc: TxtTables,
		},
		{
			name: "bad json path",
			args: args{
				filePath: "badpath/wrench.json",
			},
			wantSdc: staticDataConfig{},
			wantErr: true,
		},
		{
			name: "bad txt path",
			args: args{
				filePath: "badpath/static_data_tables.txt",
			},
			wantSdc: staticDataConfig{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			sdc, err := readStaticDataTablesFile(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("readStaticDataTablesFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(sdc, tt.wantSdc, cmpopts.EquateEmpty()) {
				t.Errorf("readStaticDataTablesFile() sdc= %v, want %v", sdc, tt.wantSdc)
			}
		})
	}
}

func Test_writeDDL(t *testing.T) {
	tempDir := t.TempDir()
	tests := []struct {
		name     string
		ddl      spanner.SchemaDDL
		expected string
	}{
		{
			name: "table",
			ddl: spanner.SchemaDDL{
				Statement:  "CREATE TABLE Singers ( SingerId INT64 ) PRIMARY KEY ( SingerId )",
				Filename:   "singers.sql",
				ObjectType: "table",
			},
			expected: filepath.Join(tempDir, "table", "singers.sql"),
		},
		{
			name: "function",
			ddl: spanner.SchemaDDL{
				Statement:  "CREATE OR REPLACE FUNCTION MyFunction() RETURNS INT64 AS (1)",
				Filename:   "myfunction.sql",
				ObjectType: "function",
			},
			expected: filepath.Join(tempDir, "function", "myfunction.sql"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := writeDDL(tt.ddl, tempDir)
			require.NoError(t, err)

			assert.FileExists(t, tt.expected)
			content, err := os.ReadFile(tt.expected)
			require.NoError(t, err)
			assert.Equal(t, tt.ddl.Statement, string(content))
		})
	}
}
