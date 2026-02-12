package service

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type logLine struct {
	Level     string `json:"level"`
	Msg       string `json:"msg"`
	Timestamp string `json:"timestamp"`
	Caller    string `json:"caller"`
}

func captureStdout(t *testing.T, f func()) []logLine {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	assert.NoError(t, err)
	os.Stdout = w

	f()

	_ = w.Close()
	os.Stdout = old

	data, err := io.ReadAll(r)
	_ = r.Close()
	assert.NoError(t, err)

	var lines []logLine
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		s := strings.TrimSpace(sc.Text())
		if s == "" {
			continue
		}
		var ll logLine
		assert.NoError(t, json.Unmarshal([]byte(s), &ll), "line: %s", s)
		lines = append(lines, ll)
	}
	assert.NoError(t, sc.Err())
	return lines
}

func TestInitLogger_LogLevel(t *testing.T) {
	const debug = "debug"

	t.Run("InfoLevelFiltersDebug", func(t *testing.T) {
		const info = "info"
		t.Setenv(logLevelEnvVar, info)
		t.Setenv(otelSdkDisabledEnvVar, "true") // avoid extra OTel logs for this test

		lines := captureStdout(t, func() {
			internal, logger, cleanup := InitLogger(context.Background(), "github.com/mydecisive/test-svc")
			assert.NotNil(t, internal)
			assert.NotNil(t, logger)

			internal.Debug("debug-internal")
			logger.Debug("debug-external")
			internal.Info("info-internal")
			logger.Info("info-external")

			assert.NotPanics(t, func() { cleanup() })
		})

		var infoMsgs, debugMsgs []string
		for _, l := range lines {
			// Only consider logs emitted from this test.
			if !strings.Contains(l.Caller, "logger_test.go") {
				continue
			}
			assert.NotEmpty(t, l.Timestamp, "missing timestamp")
			assert.NotEmpty(t, l.Caller, "missing caller")
			switch l.Level {
			case info:
				infoMsgs = append(infoMsgs, l.Msg)
			case debug:
				debugMsgs = append(debugMsgs, l.Msg)
			}
		}

		assert.Empty(t, debugMsgs, "debug logs should be filtered at info level")
		assert.Len(t, infoMsgs, 2)
		assert.Contains(t, infoMsgs, "info-internal")
		assert.Contains(t, infoMsgs, "info-external")
	})

	t.Run("DebugLevelAllowsDebug", func(t *testing.T) {
		t.Setenv(logLevelEnvVar, "debug")
		t.Setenv(otelSdkDisabledEnvVar, "true") // avoid extra OTel logs for this test

		lines := captureStdout(t, func() {
			internal, logger, cleanup := InitLogger(context.Background(), "github.com/mydecisive/test-svc")
			assert.NotNil(t, internal)
			assert.NotNil(t, logger)

			internal.Debug("debug-internal")
			logger.Debug("debug-external")
			internal.Info("info-internal")
			logger.Info("info-external")

			assert.NotPanics(t, func() { cleanup() })
		})

		var debugMsgs []string
		for _, l := range lines {
			if l.Level == debug {
				debugMsgs = append(debugMsgs, l.Msg)
			}
		}
		assert.Len(t, debugMsgs, 2)
		assert.Contains(t, debugMsgs, "debug-internal")
		assert.Contains(t, debugMsgs, "debug-external")
	})
}

func TestSetupOTel_NilLogger(t *testing.T) {
	// Keep this one to test the error path directly.
	shutdown, err := setupOTel(t.Context(), nil)
	assert.Nil(t, shutdown)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal logger is required")
}

func TestInitLogger_OTelDisabled(t *testing.T) {
	t.Setenv(otelSdkDisabledEnvVar, "true")

	lines := captureStdout(t, func() {
		_, _, cleanup := InitLogger(context.Background(), "github.com/mydecisive/test-svc")
		defer cleanup()
	})

	assert.Len(t, lines, 1)
	assert.Equal(t, "info", lines[0].Level)
	assert.Equal(t, "OTEL SDK is disabled", lines[0].Msg)
}

func TestInitLogger_OTelNoEndpointWarning(t *testing.T) {
	t.Setenv(otelSdkDisabledEnvVar, "false")
	t.Setenv(otelExporterOtlpEndpointEnvVar, "")

	lines := captureStdout(t, func() {
		_, _, cleanup := InitLogger(context.Background(), "github.com/mydecisive/test-svc")
		defer cleanup()
	})

	assert.Len(t, lines, 2)

	var infoLog, warnLog logLine
	for _, line := range lines {
		if line.Level == "info" {
			infoLog = line
		}
		if line.Level == "warn" {
			warnLog = line
		}
	}

	assert.Equal(t, "info", infoLog.Level)
	assert.Equal(t, "OTEL SDK is enabled", infoLog.Msg)

	assert.Equal(t, "warn", warnLog.Level)
	assert.Equal(t, "No OTLP endpoint is defined, but OTEL SDK is enabled.", warnLog.Msg)
}

func TestInitLogger_OTelEndpointSet(t *testing.T) {
	t.Setenv(otelSdkDisabledEnvVar, "false")
	t.Setenv(otelExporterOtlpEndpointEnvVar, "http://localhost:4318")

	lines := captureStdout(t, func() {
		_, _, cleanup := InitLogger(context.Background(), "github.com/mydecisive/test-svc")
		defer cleanup()
	})

	assert.Len(t, lines, 1)

	var warnLogs []logLine
	var infoLog logLine
	for _, line := range lines {
		if line.Level == "warn" {
			warnLogs = append(warnLogs, line)
		}
		if line.Level == "info" {
			infoLog = line
		}
	}

	assert.Empty(t, warnLogs, "should not log a warning when OTLP endpoint is set")
	assert.Equal(t, "info", infoLog.Level)
	assert.Equal(t, "OTEL SDK is enabled", infoLog.Msg)
}
