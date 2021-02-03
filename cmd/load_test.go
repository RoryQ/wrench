package cmd

import (
	"reflect"
	"testing"
)

func Test_readStaticDataTablesFile(t *testing.T) {
	type args struct {
		filePath string
	}
	JsonTables := []string{"JsonTable1", "JsonTable2"}
	TxtTables := []string{"TxtTable1", "TxtTable2"}
	tests := []struct {
		name       string
		args       args
		wantTables []string
		wantErr    bool
	}{
		{
			name: "default uses json first",
			args: args{
				filePath: "testdata/{wrench.json|static_data_tables.txt}",
			},
			wantTables: JsonTables,
		},
		{
			name: "default uses txt second",
			args: args{
				filePath: "testdata/txt/{wrench.json|static_data_tables.txt}",
			},
			wantTables: TxtTables,
		},
		{
			name: "specify json file",
			args: args{
				filePath: "testdata/wrench.json",
			},
			wantTables: JsonTables,
		},
		{
			name: "specify txt file",
			args: args{
				filePath: "testdata/static_data_tables.txt",
			},
			wantTables: TxtTables,
		},
		{
			name: "bad json path",
			args: args{
				filePath: "badpath/wrench.json",
			},
			wantTables: []string{},
			wantErr: true,
		},
		{
			name: "bad txt path",
			args: args{
				filePath: "badpath/static_data_tables.txt",
			},
			wantTables: []string{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotTables, err := readStaticDataTablesFile(tt.args.filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("readStaticDataTablesFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("readStaticDataTablesFile() gotTables = %v, want %v", gotTables, tt.wantTables)
			}
		})
	}
}
