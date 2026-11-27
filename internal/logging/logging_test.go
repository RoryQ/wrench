package logging

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCLIHandler(t *testing.T) {
	var stdout, stderr bytes.Buffer
	l := slog.New(NewCLIHandler(&stdout, &stderr))

	l.Debug("ignored debug")
	l.Info("migration applied", "version", 1, "name", "create_users")
	l.Warn("failed to release migration lock", "error", "timeout")
	l.Error("unexpected failure", "code", 500)

	assert.Equal(t, "level=INFO msg=\"migration applied\" version=1 name=create_users\n", stdout.String())
	assert.Equal(t,
		"level=WARN msg=\"failed to release migration lock\" error=timeout\nlevel=ERROR msg=\"unexpected failure\" code=500\n",
		stderr.String(),
	)
}

func TestCLIHandler_WithAttrsAndGroup(t *testing.T) {
	var stdout, stderr bytes.Buffer
	l := slog.New(NewCLIHandler(&stdout, &stderr)).With("driver", "spanner").WithGroup("db")
	l.Info("no change", "table", "users")
	l.Warn("metadata error", slog.Group("detail", "error", "denied"))

	assert.Equal(t, "level=INFO msg=\"no change\" driver=spanner db.table=users\n", stdout.String())
	assert.Equal(t, "level=WARN msg=\"metadata error\" driver=spanner db.detail.error=denied\n", stderr.String())
}

func TestCLIHandler_NilWriters(t *testing.T) {
	l := slog.New(NewCLIHandler(nil, nil))
	l.Info("should not panic")
	l.Warn("should not panic")
}
