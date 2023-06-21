package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_schema runs the schema command which requires docker
func Test_schema(t *testing.T) {
	// clean before and after
	t.Cleanup(func() { cleanup(t) })
	cleanup(t)

	// execute schema command
	cmd := schemaCmd
	cmd.Flag(flagNameDirectory).Value.Set("testdata/schema_test")
	err := schema(cmd, []string{})
	assert.NoError(t, err)

	assert.DirExists(t, "testdata/schema_test/table")
	assert.FileExists(t, "testdata/schema_test/schema.sql")

	contentSchema, err := os.ReadFile("testdata/schema_test/schema.sql")
	assert.NoError(t, err)
	contentMigration, err := os.ReadFile("testdata/schema_test/migrations/000001_create_singers.sql")
	assert.NoError(t, err)

	assert.Contains(t, string(contentSchema), string(contentMigration))
}

func cleanup(t *testing.T) {
	os.RemoveAll("testdata/schema_test/table")
	os.RemoveAll("testdata/schema_test/schema.sql")
	require.NoDirExists(t, "testdata/schema_test/table")
	require.NoFileExists(t, "testdata/schema_test/schema.sql")
}
