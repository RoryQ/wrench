package bigquery

import (
	"cmp"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// validIdentifierRegex permits standard BigQuery project, dataset, and table identifier characters
// while rejecting backticks, quotes, whitespace, and SQL metacharacters.
var validIdentifierRegex = regexp.MustCompile(`^[A-Za-z0-9_.\-:]+$`)

// Config holds connection and execution settings for the BigQuery migration driver.
type Config struct {
	Project         string
	Dataset         string
	Location        string
	CredentialsFile string
	Endpoint        string
	StmtTimeout     time.Duration
}

func validateIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("bigquery %s is required", kind)
	}
	if !validIdentifierRegex.MatchString(value) {
		return fmt.Errorf("invalid bigquery %s identifier: %q", kind, value)
	}
	return nil
}

func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("bigquery config is required")
	}
	if err := validateIdentifier("project", c.Project); err != nil {
		return err
	}
	if err := validateIdentifier("dataset", c.Dataset); err != nil {
		return err
	}
	return nil
}

func (c *Config) resolvedEndpoint() string {
	ep := cmp.Or(c.Endpoint, os.Getenv("BIGQUERY_EMULATOR_HOST"))
	if ep != "" && !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		ep = "http://" + ep
	}
	return ep
}
