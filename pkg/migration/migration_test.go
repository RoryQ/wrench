package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadMigrationsWithOptions_RawScript(t *testing.T) {
	dir := t.TempDir()

	// Mixed DDL + DML + procedural block (valid in BigQuery, would fail Spanner statement kind check)
	script := `CREATE TABLE ` + "`${PROJECT_ID}.${DATASET_ID}.users`" + ` (
  id INT64,
  name STRING
);

BEGIN
  DECLARE default_name STRING DEFAULT 'alice';
  INSERT INTO ` + "`${PROJECT_ID}.${DATASET_ID}.users`" + ` (id, name) VALUES (1, default_name);
END;`

	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001_init.sql"), []byte(script), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "R__users_view.sql"), []byte("CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.v_users` AS SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.users`;"), 0o644))

	ms, err := LoadMigrationsWithOptions(dir, LoadOptions{
		RawScript: true,
		Placeholders: PlaceholderOptions{
			ReplacementEnabled: true,
			Placeholders: map[string]string{
				"PROJECT_ID": "my-proj",
				"DATASET_ID": "my_ds",
				"LOCATION":   "US",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, ms, 2)

	assert.Equal(t, uint(1), ms[0].Version)
	assert.False(t, ms[0].IsRepeatable)
	assert.Contains(t, ms[0].RawSQL, "`my-proj.my_ds.users`")
	assert.Contains(t, ms[0].RawSQL, "DECLARE default_name STRING DEFAULT 'alice';")
	require.Len(t, ms[0].Statements, 1)
	assert.NotEmpty(t, ms[0].Checksum)

	assert.Equal(t, "users_view", ms[1].Name)
	assert.True(t, ms[1].IsRepeatable)
	assert.Equal(t, "CREATE OR REPLACE VIEW `my-proj.my_ds.v_users` AS SELECT * FROM `my-proj.my_ds.users`;", ms[1].RawSQL)
	assert.NotEmpty(t, ms[1].Checksum)
}

func TestLoadMigrationsWithOptions_RawScriptLineEndings(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "R__view.sql"), []byte("SELECT 1;\nSELECT 2;\n"), 0o644))
	msLF, err := LoadMigrationsWithOptions(dir, LoadOptions{RawScript: true})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "R__view.sql"), []byte("SELECT 1;\r\nSELECT 2;\r\n"), 0o644))
	msCRLF, err := LoadMigrationsWithOptions(dir, LoadOptions{RawScript: true})
	require.NoError(t, err)

	assert.Equal(t, msLF[0].Checksum, msCRLF[0].Checksum)
}
