package config

import (
	"io"
	"log/slog"
)

// SlogLevel maps the configured log level onto slog's level type. Validation
// has already rejected anything outside the documented set, so the default arm
// is unreachable for a validated Config; it returns info rather than panicking
// because a logger is a poor place to discover a programming error.
func (c *Config) SlogLevel() slog.Level {
	switch c.Controller.LogLevel {
	case LogLevelDebug:
		return slog.LevelDebug
	case LogLevelWarn:
		return slog.LevelWarn
	case LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewLogger builds the structured logger described by the configuration.
//
// log/slog from the standard library is used deliberately: the architecture
// asks only for structured logs with one line per reconcile, and a dependency
// would have to earn its place by doing something slog cannot.
func (c *Config) NewLogger(w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.SlogLevel()}

	var handler slog.Handler
	if c.Controller.LogFormat == LogFormatText {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}
	return slog.New(handler)
}
