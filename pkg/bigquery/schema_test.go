package bigquery

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/roryq/wrench/pkg/migration"
)

func TestLoadDDLs(t *testing.T) {
	t.Run("maps and sorts all BigQuery object types", func(t *testing.T) {
		exec := &fakeExecutor{
			querySchemaObjectsFn: func(sql string) ([]schemaObjectRow, error) {
				switch {
				case strings.Contains(sql, "INFORMATION_SCHEMA.TABLES"):
					return []schemaObjectRow{
						{Name: "Users", Type: "BASE TABLE", DDL: "CREATE TABLE `my-project.my_dataset.Users` (id INT64);"},
						{Name: "ActiveUsers", Type: "VIEW", DDL: "CREATE VIEW `my-project.my_dataset.ActiveUsers` AS SELECT * FROM Users;"},
						{Name: "DailyCounts", Type: "MATERIALIZED VIEW", DDL: "CREATE MATERIALIZED VIEW `my-project.my_dataset.DailyCounts` AS SELECT COUNT(*) FROM Users;"},
					}, nil
				case strings.Contains(sql, "INFORMATION_SCHEMA.ROUTINES"):
					return []schemaObjectRow{
						{Name: "NormalizeEmail", Type: "SCALAR FUNCTION", DDL: "CREATE FUNCTION `my-project.my_dataset.NormalizeEmail`(x STRING) RETURNS STRING AS (LOWER(x));"},
						{Name: "RecentEvents", Type: "TABLE VALUED FUNCTION", DDL: "CREATE TABLE FUNCTION `my-project.my_dataset.RecentEvents`() AS SELECT 1;"},
						{Name: "BackfillUsers", Type: "PROCEDURE", DDL: "CREATE PROCEDURE `my-project.my_dataset.BackfillUsers`() BEGIN SELECT 1; END;"},
					}, nil
				case strings.Contains(sql, "INFORMATION_SCHEMA.SEARCH_INDEXES"):
					return []schemaObjectRow{
						{Name: "idx_users", Type: "SEARCH_INDEX", DDL: "CREATE SEARCH INDEX idx_users ON `my-project.my_dataset.Users`(ALL COLUMNS);"},
					}, nil
				default:
					return nil, nil
				}
			},
		}
		c := newTestClient(exec)

		ddls, err := c.LoadDDLs(t.Context())
		require.NoError(t, err)
		require.Len(t, ddls, 7)

		// Sorted by (ObjectType, Filename)
		want := []migration.SchemaDDL{
			{ObjectType: ObjectTypeFunction, Filename: "normalizeemail.sql", Statement: "CREATE FUNCTION `my-project.my_dataset.NormalizeEmail`(x STRING) RETURNS STRING AS (LOWER(x));"},
			{ObjectType: ObjectTypeMaterializedView, Filename: "dailycounts.sql", Statement: "CREATE MATERIALIZED VIEW `my-project.my_dataset.DailyCounts` AS SELECT COUNT(*) FROM Users;"},
			{ObjectType: ObjectTypeProcedure, Filename: "backfillusers.sql", Statement: "CREATE PROCEDURE `my-project.my_dataset.BackfillUsers`() BEGIN SELECT 1; END;"},
			{ObjectType: ObjectTypeSearchIndex, Filename: "idx_users.sql", Statement: "CREATE SEARCH INDEX idx_users ON `my-project.my_dataset.Users`(ALL COLUMNS);"},
			{ObjectType: ObjectTypeTable, Filename: "users.sql", Statement: "CREATE TABLE `my-project.my_dataset.Users` (id INT64);"},
			{ObjectType: ObjectTypeTableFunction, Filename: "recentevents.sql", Statement: "CREATE TABLE FUNCTION `my-project.my_dataset.RecentEvents`() AS SELECT 1;"},
			{ObjectType: ObjectTypeView, Filename: "activeusers.sql", Statement: "CREATE VIEW `my-project.my_dataset.ActiveUsers` AS SELECT * FROM Users;"},
		}
		assert.Equal(t, want, ddls)
	})

	t.Run("best-effort when ROUTINES or SEARCH_INDEXES fail", func(t *testing.T) {
		exec := &fakeExecutor{
			querySchemaObjectsFn: func(sql string) ([]schemaObjectRow, error) {
				switch {
				case strings.Contains(sql, "INFORMATION_SCHEMA.TABLES"):
					return []schemaObjectRow{
						{Name: "events", Type: "BASE TABLE", DDL: "CREATE TABLE events (id INT64);"},
					}, nil
				default:
					return nil, errors.New("unsupported information schema view")
				}
			},
		}
		c := newTestClient(exec)

		ddls, err := c.LoadDDLs(t.Context())
		require.NoError(t, err)
		require.Len(t, ddls, 1)
		assert.Equal(t, ObjectTypeTable, ddls[0].ObjectType)
		assert.Equal(t, "events.sql", ddls[0].Filename)
	})

	t.Run("errors when INFORMATION_SCHEMA.TABLES fails", func(t *testing.T) {
		exec := &fakeExecutor{
			querySchemaObjectsFn: func(_ string) ([]schemaObjectRow, error) {
				return nil, errors.New("permission denied")
			},
		}
		c := newTestClient(exec)

		_, err := c.LoadDDLs(t.Context())
		require.Error(t, err)
		var bqErr *Error
		require.ErrorAs(t, err, &bqErr)
		assert.Equal(t, ErrorCodeLoadSchema, bqErr.Code)
	})
}

func TestLoadDDL(t *testing.T) {
	exec := &fakeExecutor{
		querySchemaObjectsFn: func(sql string) ([]schemaObjectRow, error) {
			if strings.Contains(sql, "INFORMATION_SCHEMA.TABLES") {
				return []schemaObjectRow{
					{Name: "events", Type: "BASE TABLE", DDL: "CREATE TABLE events (id INT64)"},
					{Name: "v_events", Type: "VIEW", DDL: "CREATE VIEW v_events AS SELECT * FROM events;"},
				}, nil
			}
			return nil, nil
		},
	}
	c := newTestClient(exec)

	out, err := c.LoadDDL(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE events (id INT64);\n\nCREATE VIEW v_events AS SELECT * FROM events;\n", string(out))
}
