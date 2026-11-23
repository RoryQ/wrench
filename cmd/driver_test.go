package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDriverCmd(t *testing.T, driverVal, instanceVal, datasetVal string, args ...string) *cobra.Command {
	t.Helper()
	c := &cobra.Command{Use: "test"}
	c.Flags().String(flagNameDriver, driverVal, "")
	c.Flags().String(flagNameProject, "", "")
	c.Flags().String(flagNameInstance, instanceVal, "")
	c.Flags().String(flagNameDatabase, "", "")
	c.Flags().String(flagNameDataset, datasetVal, "")
	c.Flags().String(flagCredentialsFile, "", "")
	if len(args) > 0 {
		require.NoError(t, c.ParseFlags(args))
	}
	return c
}

func TestResolveDriver(t *testing.T) {
	tests := []struct {
		name     string
		driver   string
		instance string
		dataset  string
		args     []string
		want     string
		wantErr  string
	}{
		{
			name:   "explicit spanner",
			driver: "spanner",
			want:   driverSpanner,
		},
		{
			name:   "explicit bigquery (case-insensitive)",
			driver: "BigQuery",
			want:   driverBigQuery,
		},
		{
			name:    "auto-detect bigquery from dataset env when instance empty",
			dataset: "my_dataset",
			want:    driverBigQuery,
		},
		{
			name:     "auto-detect spanner when instance env set",
			instance: "my-instance",
			want:     driverSpanner,
		},
		{
			name:     "explicit --dataset overrides SPANNER_INSTANCE_ID env",
			instance: "env-instance",
			args:     []string{"--dataset", "cli_dataset"},
			want:     driverBigQuery,
		},
		{
			name:    "explicit --instance overrides BIGQUERY_DATASET_ID env",
			dataset: "env_dataset",
			args:    []string{"--instance", "cli-instance"},
			want:    driverSpanner,
		},
		{
			name:    "both --dataset and --instance explicitly passed errors without --driver",
			args:    []string{"--dataset", "cli_dataset", "--instance", "cli-instance"},
			wantErr: "both --dataset and --instance were specified",
		},
		{
			name: "defaults to spanner when nothing set",
			want: driverSpanner,
		},
		{
			name:    "unsupported driver returns error",
			driver:  "postgres",
			wantErr: `unsupported driver "postgres"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestDriverCmd(t, tc.driver, tc.instance, tc.dataset, tc.args...)
			got, err := resolveDriver(c)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestNewSpannerClientRejectsBigQueryDriver(t *testing.T) {
	c := newTestDriverCmd(t, "bigquery", "inst", "ds")
	c.Use = "drop"
	_, err := newSpannerClient(t.Context(), c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `command "drop" is not supported for the bigquery driver`)
}

func TestResolvedBigQueryProject(t *testing.T) {
	t.Run("prefers BIGQUERY_PROJECT_ID over SPANNER_PROJECT_ID default when --project not explicitly passed", func(t *testing.T) {
		t.Setenv("BIGQUERY_PROJECT_ID", "bq-proj")
		c := newTestDriverCmd(t, "bigquery", "", "ds")
		require.NoError(t, c.Flags().Set(flagNameProject, "spanner-proj"))
		// Reset Changed to simulate default value from SPANNER_PROJECT_ID env
		c.Flags().Lookup(flagNameProject).Changed = false

		assert.Equal(t, "bq-proj", resolvedBigQueryProject(c))
	})

	t.Run("respects explicit --project flag over BIGQUERY_PROJECT_ID", func(t *testing.T) {
		t.Setenv("BIGQUERY_PROJECT_ID", "bq-proj")
		c := newTestDriverCmd(t, "bigquery", "", "ds", "--project", "explicit-proj")
		assert.Equal(t, "explicit-proj", resolvedBigQueryProject(c))
	})
}

func TestEnvHelpers(t *testing.T) {
	t.Run("defaultProjectID precedence", func(t *testing.T) {
		t.Setenv("SPANNER_PROJECT_ID", "spanner-proj")
		t.Setenv("BIGQUERY_PROJECT_ID", "bq-proj")
		t.Setenv("GOOGLE_CLOUD_PROJECT", "gcp-proj")
		assert.Equal(t, "spanner-proj", defaultProjectID())

		t.Setenv("SPANNER_PROJECT_ID", "")
		assert.Equal(t, "bq-proj", defaultProjectID())

		t.Setenv("BIGQUERY_PROJECT_ID", "")
		assert.Equal(t, "gcp-proj", defaultProjectID())
	})

	t.Run("bigQueryDatasetID precedence", func(t *testing.T) {
		t.Setenv("BIGQUERY_DATASET_ID", "ds-id")
		t.Setenv("BIGQUERY_DATASET", "ds-alt")
		assert.Equal(t, "ds-id", bigQueryDatasetID())

		t.Setenv("BIGQUERY_DATASET_ID", "")
		assert.Equal(t, "ds-alt", bigQueryDatasetID())
	})

	t.Run("bigQueryLocation", func(t *testing.T) {
		t.Setenv("BIGQUERY_LOCATION", "australia-southeast1")
		assert.Equal(t, "australia-southeast1", bigQueryLocation())
	})
}
