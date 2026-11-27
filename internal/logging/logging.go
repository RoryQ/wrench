package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// levelRouterHandler routes INFO/DEBUG records to stdout and WARN/ERROR records to stderr,
// delegating formatting and slog.Handler semantics to slog.NewTextHandler.
type levelRouterHandler struct {
	stdout slog.Handler
	stderr slog.Handler
}

// NewCLIHandler returns a slog.Handler that writes INFO records to stdout and
// WARN/ERROR records to stderr using Go's text format without timestamps.
func NewCLIHandler(stdout, stderr io.Writer) slog.Handler {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}
	return &levelRouterHandler{
		stdout: slog.NewTextHandler(stdout, opts),
		stderr: slog.NewTextHandler(stderr, opts),
	}
}

// DefaultLogger returns the default CLI logger writing info to os.Stdout
// and warnings/errors to os.Stderr.
func DefaultLogger() *slog.Logger {
	return slog.New(NewCLIHandler(os.Stdout, os.Stderr))
}

func (h *levelRouterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if level >= slog.LevelWarn {
		return h.stderr.Enabled(ctx, level)
	}
	return h.stdout.Enabled(ctx, level)
}

func (h *levelRouterHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		return h.stderr.Handle(ctx, r)
	}
	return h.stdout.Handle(ctx, r)
}

func (h *levelRouterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelRouterHandler{
		stdout: h.stdout.WithAttrs(attrs),
		stderr: h.stderr.WithAttrs(attrs),
	}
}

func (h *levelRouterHandler) WithGroup(name string) slog.Handler {
	return &levelRouterHandler{
		stdout: h.stdout.WithGroup(name),
		stderr: h.stderr.WithGroup(name),
	}
}
