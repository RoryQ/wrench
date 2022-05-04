package cmd

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
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
