package subscriber

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

//nolint:gocritic
func runJetStream(t *testing.T) *server.Server {
	t.Helper()
	tempDir := t.TempDir()
	ns, err := server.NewServer(&server.Options{
		JetStream: true,
		StoreDir:  tempDir,
		Port:      -1, // pick a random free port
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not start")
	}

	t.Setenv("NATS_URL", ns.ClientURL())

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns
}

func TestNewSubscriber_WrongConsumerGroup(t *testing.T) {
	srv := runJetStream(t)
	defer srv.Shutdown()

	logger := zap.NewNop()

	sub, err := NewSubscriber(context.Background(), logger, "test-client")
	require.NoError(t, err)

	err = sub.Subscribe(t.Context(), "wrong_consumer_group", "alerts", func(ev eventing.MdaiEvent) error {
		return nil
	})
	require.Error(t, err, "expected an error when config loading fails")
}

func TestSubscriber_SubscribeValidationErrors(t *testing.T) {
	_ = runJetStream(t)

	logger := zap.NewNop()
	sub, err := NewSubscriber(context.Background(), logger, "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	validInvoker := func(eventing.MdaiEvent) error { return nil }

	// groupName required
	err = sub.Subscribe(ctx, "", "alerts", validInvoker)
	require.EqualError(t, err, "groupName is required")

	// invoker required
	err = sub.Subscribe(ctx, eventing.AlertConsumerGroupName.String(), "alerts", nil)
	require.EqualError(t, err, "invoker is required")

	// dlqSubject required
	err = sub.Subscribe(ctx, eventing.AlertConsumerGroupName.String(), "", validInvoker)
	require.EqualError(t, err, "dlqSubject is required")
}

func TestSubscriber_SubscribeSuccessfully(t *testing.T) {
	srv := runJetStream(t)
	defer srv.Shutdown()

	logger := zap.NewNop()

	sub, err := NewSubscriber(context.Background(), logger, "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alerts", func(ev eventing.MdaiEvent) error {
		fmt.Printf("Received alert: %+v\n", ev)
		return nil
	})
	require.NoError(t, err)
}
