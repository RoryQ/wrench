// Copyright (c) 2020 Mercari, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

	c.Flag(flagNameProject).Value.Set("schema")
	c.Flag(flagNameInstance).Value.Set("schema")
	c.Flag(flagNameDatabase).Value.Set("schema")

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
