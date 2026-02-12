package valkey

import (
	"time"

	"github.com/mydecisive/mdai-data-core/helpers"
)

const (
	defaultEndpoint = "mdai-valkey-primary.mdai.svc.cluster.local:6379"

	defaultInitialBackoff    = 5 * time.Second
	defaultMaxElapsedBackoff = 3 * time.Minute

	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"
)

type Config struct {
	Password    string
	InitAddress []string

	InitialBackoffInterval time.Duration
	MaxBackoffElapsedTime  time.Duration
}

func NewConfig() Config {
	return Config{
		InitAddress:            []string{helpers.GetEnvVariableWithDefault(valkeyEndpointEnvVarKey, defaultEndpoint)},
		Password:               helpers.GetEnvVariableWithDefault(valkeyPasswordEnvVarKey, ""),
		InitialBackoffInterval: defaultInitialBackoff,
		MaxBackoffElapsedTime:  defaultMaxElapsedBackoff,
	}
}
