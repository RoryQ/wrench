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
	_ = cmd.Flag(flagNameDirectory).Value.Set("testdata/schema_test")
	_ = cmd.Flag(flagNameProject).Value.Set("test-project")
	_ = cmd.Flag(flagNameInstance).Value.Set("my-instance")
	err := schema(cmd, []string{})
	assert.NoError(t, err)

	assert.DirExists(t, "testdata/schema_test/table")
	assert.FileExists(t, "testdata/schema_test/schema.sql")

	contentSchema, err := os.ReadFile("testdata/schema_test/schema.sql")
	assert.NoError(t, err)
	contentMigration, err := os.ReadFile("testdata/schema_test/migrations/000001_create_singers.sql")
	assert.NoError(t, err)

	assert.Contains(t, string(contentSchema), string(contentMigration))

	assert.DirExists(t, "testdata/schema_test/model")
	assert.FileExists(t, "testdata/schema_test/model/custom_ai_model.sql")

	contentAIModelOutput, err := os.ReadFile("testdata/schema_test/model/custom_ai_model.sql")
	assert.NoError(t, err)
	// replacements have been applied
	assert.Contains(t, string(contentAIModelOutput), `endpoint = '//aiplatform.googleapis.com/projects/test-project/locations/us-central1/publishers/google/models/my-instance-database-model'`)

	assert.DirExists(t, "testdata/schema_test/change_stream")
	assert.FileExists(t, "testdata/schema_test/change_stream/alltables.sql")

	contentChangeStream, err := os.ReadFile("testdata/schema_test/change_stream/alltables.sql")
	assert.NoError(t, err)
	assert.Contains(t, string(contentChangeStream), "CREATE CHANGE STREAM AllTables")

	assert.FileExists(t, "testdata/schema_test/table/advancedsyntax.sql")
	contentAdvancedTable, err := os.ReadFile("testdata/schema_test/table/advancedsyntax.sql")
	assert.NoError(t, err)
	assert.Contains(t, string(contentAdvancedTable), "JSON")
	assert.Contains(t, string(contentAdvancedTable), "CHECK(Id > 0)")

	assert.DirExists(t, "testdata/schema_test/sequence")
	assert.FileExists(t, "testdata/schema_test/sequence/mysequence.sql")
	contentSequence, err := os.ReadFile("testdata/schema_test/sequence/mysequence.sql")
	assert.NoError(t, err)
	assert.Contains(t, string(contentSequence), "CREATE SEQUENCE MySequence")
}

func cleanup(t *testing.T) {
	os.RemoveAll("testdata/schema_test/table")
	os.RemoveAll("testdata/schema_test/schema.sql")
	os.RemoveAll("testdata/schema_test/model")
	os.RemoveAll("testdata/schema_test/change_stream")
	os.RemoveAll("testdata/schema_test/sequence")
	require.NoDirExists(t, "testdata/schema_test/table")
	require.NoFileExists(t, "testdata/schema_test/schema.sql")
	require.NoDirExists(t, "testdata/schema_test/model")
	require.NoDirExists(t, "testdata/schema_test/change_stream")
	require.NoDirExists(t, "testdata/schema_test/sequence")
	require.NoFileExists(t, "testdata/schema_test/custom_ai_model.sql")
}
