package cmd

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/spf13/cobra"

	"github.com/roryq/wrench/internal/graceful"
)

var gracefulSchemaTasks = graceful.OnShutdown{}

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Runs the migrations against a dockerised spanner emulator, then loads the schema and static data to disk. (Requires docker)",
	RunE:  schema,
}

func init() {
	// schema flags
	schemaCmd.Flags().String(flagSpannerEmulatorImage, "roryq/spanner-emulator:latest", "Spanner emulator image to use. Override this to pin version or change registry.")

	// copy migrate up flags
	schemaCmd.Flags().AddFlagSet(findCommand("up").LocalFlags())
}

func findCommand(name string) *cobra.Command {
	for _, command := range migrateCmd.Commands() {
		if command.Name() == name {
			return command
		}
	}
	return nil
}

func schema(c *cobra.Command, args []string) error {
	defer gracefulSchemaTasks.Exit()

	f := c.Flag(flagSpannerEmulatorImage)
	_, err := runSpannerEmulator(f.Value.String())
	if err != nil {
		return err
	}

	_ = c.Flag(flagNameProject).Value.Set("schema")
	_ = c.Flag(flagNameInstance).Value.Set("schema")
	_ = c.Flag(flagNameDatabase).Value.Set("schema")

	// run migrations
	if err := migrateUp(c, args); err != nil {
		return err
	}

	// load schema
	if err := load(c, args); err != nil {
		return err
	}

	// load discrete
	if err := loadDiscrete(c, args); err != nil {
		return err
	}

	return err
}

func connectDockerPool() (*dockertest.Pool, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	return pool, pool.Client.Ping()
}

func runSpannerEmulator(image string) (*spannerEmulator, error) {
	pool, err := connectDockerPool()
	if err != nil {
		return nil, err
	}

	repo, tag, _ := strings.Cut(image, ":")
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: repo,
		Tag:        tag,
		Env: []string{
			"SPANNER_PROJECT_ID=schema",
			"SPANNER_INSTANCE_ID=schema",
			"SPANNER_DATABASE_ID=schema",
		},
	},
		func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.NeverRestart()
		},
	)
	if err != nil {
		return nil, err
	}

	gracefulSchemaTasks.Do(func() {
		if err = container.Close(); err != nil {
			println("error during spanner container shutdown", err.Error())
		}
	})

	pool.MaxWait = time.Minute
	err = pool.Retry(func() error {
		hcheck := fmt.Sprintf("http://localhost:%s/v1/projects/test-project/instanceConfigs", container.GetPort("9020/tcp"))
		_, err := http.Get(hcheck)
		return err
	})
	if err != nil {
		return nil, err
	}
	unset, err := setenv("SPANNER_EMULATOR_HOST", container.GetHostPort("9010/tcp"))
	if err != nil {
		return nil, err
	}
	gracefulSchemaTasks.Do(unset)

	return &spannerEmulator{container: container, pool: pool}, nil
}

type spannerEmulator struct {
	container *dockertest.Resource
	pool      *dockertest.Pool
}

func (s *spannerEmulator) SpannerEmulatorHost() string {
	return s.container.GetHostPort("9010/tcp")
}

func setenv(key, value string) (func(), error) {
	prevValue, ok := os.LookupEnv(key)

	if err := os.Setenv(key, value); err != nil {
		return func() {}, err
	}

	return func() {
		if ok {
			os.Setenv(key, prevValue)
		} else {
			os.Unsetenv(key)
		}
	}, nil
}
