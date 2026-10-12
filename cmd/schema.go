package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/moby/moby/api/types/container"
	"github.com/ory/dockertest/v4"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

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
	if up := findCommand("up"); up != nil {
		schemaCmd.Flags().AddFlagSet(up.LocalFlags())
	}
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

	ctx := c.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	projectId := setIfUnset(c.Flag(flagNameProject), "project")
	instanceId := setIfUnset(c.Flag(flagNameInstance), "instance")
	databaseId := setIfUnset(c.Flag(flagNameDatabase), "database")

	f := c.Flag(flagSpannerEmulatorImage)
	_, err := runSpannerEmulator(ctx, f.Value.String(), projectId, instanceId, databaseId)
	if err != nil {
		return err
	}

	// create database
	{
		client, err := newSpannerClient(ctx, c)
		if err != nil {
			return err
		}
		if err := client.CreateDatabase(ctx, nil, nil); err != nil {
			if status.Code(err) != codes.AlreadyExists {
				_ = client.Close()
				return err
			}
		}
		_ = client.Close()
	}

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

func setIfUnset(f *pflag.Flag, val string) string {
	if f.Value.String() == "" {
		_ = f.Value.Set(val)
	}
	return f.Value.String()
}

func connectDockerPool(ctx context.Context) (dockertest.ClosablePool, error) {
	pool, err := dockertest.NewPool(ctx, "")
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func runSpannerEmulator(ctx context.Context, image string, projectId, instanceId, databaseId string) (*spannerEmulator, error) {
	pool, err := connectDockerPool(ctx)
	if err != nil {
		return nil, err
	}

	repo, tag, _ := strings.Cut(image, ":")
	opts := []dockertest.RunOption{
		dockertest.WithoutReuse(),
		dockertest.WithEnv([]string{
			"SPANNER_PROJECT_ID=" + projectId,
			"SPANNER_INSTANCE_ID=" + instanceId,
			"SPANNER_DATABASE_ID=" + databaseId,
		}),
		dockertest.WithHostConfig(func(config *container.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyDisabled}
		}),
	}
	if tag != "" {
		opts = append(opts, dockertest.WithTag(tag))
	}

	container, err := pool.Run(ctx, repo, opts...)
	if err != nil {
		return nil, err
	}

	gracefulSchemaTasks.Do(func() {
		if err = container.Close(context.Background()); err != nil {
			println("error during spanner container shutdown", err.Error())
		}
		if err = pool.Close(context.Background()); err != nil {
			println("error during docker pool shutdown", err.Error())
		}
	})

	err = pool.Retry(ctx, time.Minute, func() error {
		hcheck := fmt.Sprintf("http://localhost:%s/v1/projects/test-project/instanceConfigs", container.GetPort("9020/tcp"))
		resp, err := http.Get(hcheck)
		if err != nil {
			return err
		}
		return resp.Body.Close()
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
	container dockertest.ClosableResource
	pool      dockertest.ClosablePool
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
