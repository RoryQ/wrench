package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/roryq/wrench/pkg/spanner"
)

// Test_schema runs the schema command which requires docker
func Test_schema(t *testing.T) {
	//clean before and after
	t.Cleanup(func() { cleanup(t) })
	cleanup(t)

	// execute schema command
	const definitionsDir = "testdata/schema_test/definitions"

	cmd := schemaCmd
	_ = cmd.Flag(flagNameDirectory).Value.Set("testdata/schema_test")
	_ = cmd.Flag(flagNameOutputDir).Value.Set(definitionsDir)
	_ = cmd.Flag(flagNameProject).Value.Set("test-project")
	_ = cmd.Flag(flagNameInstance).Value.Set("my-instance")
	err := schema(cmd, []string{})
	assert.NoError(t, err)

	assert.FileExists(t, "testdata/schema_test/schema.sql")

	contentSchema, err := os.ReadFile("testdata/schema_test/schema.sql")
	assert.NoError(t, err)
	contentMigration, err := os.ReadFile("testdata/schema_test/migrations/000001_create_singers.sql")
	assert.NoError(t, err)

	assert.Contains(t, string(contentSchema), string(contentMigration))

	// check all object types that should have been created
	expectedFiles := []string{
		filepath.Join(spanner.ObjectTypeTable, "singers.sql"),
		filepath.Join(spanner.ObjectTypeModel, "custom_ai_model.sql"),
		filepath.Join(spanner.ObjectTypeView, "singernames.sql"),
		filepath.Join(spanner.ObjectTypeChangeStream, "mystream.sql"),
		filepath.Join(spanner.ObjectTypeSequence, "mysequence.sql"),
		filepath.Join(spanner.ObjectTypeIndex, "ix_singers_firstname.sql"),
		filepath.Join(spanner.ObjectTypeFunction, "addone.sql"),
		filepath.Join(spanner.ObjectTypeFunction, "my_schema.addtwo.sql"),
		filepath.Join(spanner.ObjectTypeFunction, "my_schema.multiply.sql"),
		filepath.Join(spanner.ObjectTypeSchema, "my_schema.sql"),
	}
	for _, file := range expectedFiles {
		assert.FileExists(t, filepath.Join(definitionsDir, file))
	}

	contentAIModelOutput, err := os.ReadFile(filepath.Join(definitionsDir, "model/custom_ai_model.sql"))
	assert.NoError(t, err)
	// replacements have been applied
	assert.Contains(t, string(contentAIModelOutput), `endpoint = '//aiplatform.googleapis.com/projects/test-project/locations/us-central1/publishers/google/models/my-instance-database-model'`)
}

func cleanup(t *testing.T) {
	dir := "testdata/schema_test"
	os.Remove(filepath.Join(dir, "schema.sql"))
	os.RemoveAll(filepath.Join(dir, "definitions"))

	// Use spanner.AllObjectTypes to clean up all potential directories
	for _, target := range append([]string{"static_data"}, spanner.AllObjectTypes...) {
		os.RemoveAll(filepath.Join(dir, target))
	}
}
