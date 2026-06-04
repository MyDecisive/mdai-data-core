package valkey

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// helper to save+restore a single env var
func saveEnv(key string) (restore func()) {
	prevVal, prevOK := os.LookupEnv(key)
	return func() {
		if prevOK {
			_ = os.Setenv(key, prevVal)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

func TestNewConfig_DefaultsWhenEnvUnset(t *testing.T) {
	restoreEnd := saveEnv(valkeyEndpointEnvVarKey)
	restorePass := saveEnv(valkeyPasswordEnvVarKey)
	defer restoreEnd()
	defer restorePass()

	_ = os.Unsetenv(valkeyEndpointEnvVarKey)
	_ = os.Unsetenv(valkeyPasswordEnvVarKey)

	cfg := NewConfig()

	require.Equal(t, []string{defaultEndpoint}, cfg.InitAddress)
	require.Empty(t, cfg.Password)
	require.Equal(t, defaultInitialBackoff, cfg.InitialBackoffInterval)
	require.Equal(t, defaultMaxElapsedBackoff, cfg.MaxBackoffElapsedTime)
}

func TestNewConfig_UsesEnvOverrides(t *testing.T) {
	restoreEnd := saveEnv(valkeyEndpointEnvVarKey)
	restorePass := saveEnv(valkeyPasswordEnvVarKey)
	defer restoreEnd()
	defer restorePass()

	const (
		customEndpoint = "valkey.svc.cluster.local:6380"
		customPassword = "hunter2"
	)
	require.NoError(t, os.Setenv(valkeyEndpointEnvVarKey, customEndpoint))
	require.NoError(t, os.Setenv(valkeyPasswordEnvVarKey, customPassword))

	cfg := NewConfig()

	require.Equal(t, []string{customEndpoint}, cfg.InitAddress)
	require.Equal(t, customPassword, cfg.Password)
	require.Equal(t, defaultInitialBackoff, cfg.InitialBackoffInterval)
	require.Equal(t, defaultMaxElapsedBackoff, cfg.MaxBackoffElapsedTime)
}

func TestNewConfig_EmptyStringInEnvDoesNotFallback(t *testing.T) {
	restoreEnd := saveEnv(valkeyEndpointEnvVarKey)
	restorePass := saveEnv(valkeyPasswordEnvVarKey)
	defer restoreEnd()
	defer restorePass()

	// Set env vars to empty strings: LookupEnv => ("" , true)
	require.NoError(t, os.Setenv(valkeyEndpointEnvVarKey, ""))
	require.NoError(t, os.Setenv(valkeyPasswordEnvVarKey, ""))

	cfg := NewConfig()

	// Because GetEnvVariableWithDefault uses LookupEnv, empty string is respected (no default fallback)
	require.Equal(t, []string{""}, cfg.InitAddress)
	require.Empty(t, cfg.Password)

	// Durations remain defaults
	require.Equal(t, defaultInitialBackoff, cfg.InitialBackoffInterval)
	require.Equal(t, defaultMaxElapsedBackoff, cfg.MaxBackoffElapsedTime)
}

// Optional guard: ensure we didn't accidentally change default constants.
func TestNewConfig_DefaultDurationsConst(t *testing.T) {
	require.Equal(t, 5*time.Second, defaultInitialBackoff)
	require.Equal(t, 3*time.Minute, defaultMaxElapsedBackoff)
}
