package bigquery

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/roryq/wrench/pkg/migration"
)

const (
	ObjectTypeTable            = "table"
	ObjectTypeView             = "view"
	ObjectTypeMaterializedView = "materialized_view"
	ObjectTypeFunction         = "function"
	ObjectTypeTableFunction    = "table_function"
	ObjectTypeProcedure        = "procedure"
	ObjectTypeSearchIndex      = "search_index"
)

// AllObjectTypes lists all discrete schema subdirectories produced by the BigQuery driver.
var AllObjectTypes = []string{
	ObjectTypeTable,
	ObjectTypeView,
	ObjectTypeMaterializedView,
	ObjectTypeFunction,
	ObjectTypeTableFunction,
	ObjectTypeProcedure,
	ObjectTypeSearchIndex,
}

type schemaObjectRow struct {
	Name string `bigquery:"name"`
	Type string `bigquery:"type"`
	DDL  string `bigquery:"ddl"`
}

func (c *Client) infoSchemaRef(view string) string {
	return fmt.Sprintf("`%s.%s.INFORMATION_SCHEMA.%s`", c.config.Project, c.config.Dataset, view)
}

func mapBigQueryObjectType(rawType string) string {
	switch strings.ToUpper(strings.TrimSpace(rawType)) {
	case "BASE TABLE", "CLONE", "SNAPSHOT", "EXTERNAL":
		return ObjectTypeTable
	case "VIEW":
		return ObjectTypeView
	case "MATERIALIZED VIEW":
		return ObjectTypeMaterializedView
	case "TABLE VALUED FUNCTION":
		return ObjectTypeTableFunction
	case "PROCEDURE":
		return ObjectTypeProcedure
	case "FUNCTION", "SCALAR FUNCTION", "AGGREGATE FUNCTION":
		return ObjectTypeFunction
	case "SEARCH_INDEX", "SEARCH INDEX":
		return ObjectTypeSearchIndex
	default:
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(rawType), " ", "_"))
	}
}

// LoadDDLs queries BigQuery INFORMATION_SCHEMA views for the dataset and returns
// discrete DDL objects suitable for exporting one file per object.
func (c *Client) LoadDDLs(ctx context.Context) ([]migration.SchemaDDL, error) {
	tablesSQL := fmt.Sprintf(
		`SELECT table_name AS name, table_type AS type, ddl FROM %s WHERE ddl IS NOT NULL ORDER BY table_name ASC`,
		c.infoSchemaRef("TABLES"),
	)
	tableRows, err := c.exec.querySchemaObjects(ctx, tablesSQL)
	if err != nil {
		return nil, &Error{
			Code: ErrorCodeLoadSchema,
			err:  fmt.Errorf("failed to query INFORMATION_SCHEMA.TABLES: %w", err),
		}
	}

	var allRows []schemaObjectRow
	allRows = append(allRows, tableRows...)

	routinesSQL := fmt.Sprintf(
		`SELECT routine_name AS name, routine_type AS type, ddl FROM %s WHERE ddl IS NOT NULL ORDER BY routine_name ASC`,
		c.infoSchemaRef("ROUTINES"),
	)
	routineRows, err := c.exec.querySchemaObjects(ctx, routinesSQL)
	if err != nil {
		slog.WarnContext(ctx, "failed to query INFORMATION_SCHEMA.ROUTINES", "error", err)
	} else {
		allRows = append(allRows, routineRows...)
	}

	searchIndexesSQL := fmt.Sprintf(
		`SELECT index_name AS name, 'SEARCH_INDEX' AS type, ddl FROM %s WHERE ddl IS NOT NULL ORDER BY index_name ASC`,
		c.infoSchemaRef("SEARCH_INDEXES"),
	)
	indexRows, err := c.exec.querySchemaObjects(ctx, searchIndexesSQL)
	if err != nil {
		slog.WarnContext(ctx, "failed to query INFORMATION_SCHEMA.SEARCH_INDEXES", "error", err)
	} else {
		allRows = append(allRows, indexRows...)
	}

	ddls := make([]migration.SchemaDDL, 0, len(allRows))
	for _, row := range allRows {
		stmt := strings.TrimSpace(row.DDL)
		if stmt == "" || row.Name == "" {
			continue
		}
		objType := mapBigQueryObjectType(row.Type)
		if objType == "" {
			continue
		}
		ddls = append(ddls, migration.SchemaDDL{
			Statement:  stmt,
			ObjectType: objType,
			Filename:   fmt.Sprintf("%s.sql", strings.ToLower(row.Name)),
		})
	}

	slices.SortFunc(ddls, func(a, b migration.SchemaDDL) int {
		return cmp.Or(
			cmp.Compare(a.ObjectType, b.ObjectType),
			cmp.Compare(a.Filename, b.Filename),
		)
	})

	return ddls, nil
}

// LoadDDL exports all dataset object DDLs concatenated into a single SQL document.
func (c *Client) LoadDDL(ctx context.Context) ([]byte, error) {
	ddls, err := c.LoadDDLs(ctx)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for i, d := range ddls {
		stmt := strings.TrimSpace(d.Statement)
		if !strings.HasSuffix(stmt, ";") {
			stmt += ";"
		}
		b.WriteString(stmt)
		if i < len(ddls)-1 {
			b.WriteString("\n\n")
		} else {
			b.WriteString("\n")
		}
	}

	return []byte(b.String()), nil
}
