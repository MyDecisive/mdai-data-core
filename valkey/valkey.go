package valkey

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

// Dialer used to inject the dialer function for testing purposes
type Dialer func(valkey.ClientOption) (valkey.Client, error)

type Option func(*settings)

type settings struct {
	dialer Dialer
}

func WithDialer(d Dialer) Option { return func(s *settings) { s.dialer = d } }

func Init(ctx context.Context, logger *zap.Logger, cfg Config, opts ...Option) (valkey.Client, error) {
	s := settings{
		dialer: valkey.NewClient, // production default
	}
	for _, o := range opts {
		o(&s)
	}

	retryCount := 0
	connectToValkey := func() (valkey.Client, error) {
		client, err := s.dialer(valkey.ClientOption{
			InitAddress: cfg.InitAddress,
			Password:    cfg.Password,
		})
		if err != nil {
			retryCount++
			return nil, err
		}

		return client, nil
	}

	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = cfg.InitialBackoffInterval

	notifyFunc := func(err error, duration time.Duration) {
		logger.Warn("Failed to connect to Valkey. Retrying...",
			zap.Error(err),
			zap.Int("retry_count", retryCount),
			zap.Duration("duration", duration))
	}

	client, err := backoff.Retry(
		ctx,
		connectToValkey,
		backoff.WithBackOff(exponentialBackoff),
		backoff.WithMaxElapsedTime(cfg.MaxBackoffElapsedTime),
		backoff.WithNotify(notifyFunc),
	)
	if err != nil {
		return nil, err
	}

	logger.Info("Connected to Valkey successfully", zap.Int("retry_count", retryCount))

	return client, nil
}
