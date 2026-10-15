package opamp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type MockConnection struct {
	shouldError bool
	idToVerify  string
	t           *testing.T
}

func (*MockConnection) Connection() net.Conn {
	return nil
}

func (m *MockConnection) Send(ctx context.Context, message *protobufs.ServerToAgent) error {
	if m.shouldError {
		return assert.AnError
	}
	assert.Equal(m.t, m.idToVerify, string(message.GetInstanceUid()))
	return nil
}

func (m *MockConnection) Disconnect() error {
	if m.shouldError {
		return assert.AnError
	}
	return nil
}

func TestAddConnection(t *testing.T) {
	t.Run("happy path - missing hostname", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		theUUID := uuid.New()
		serializedUUID, err := theUUID.MarshalText()
		require.NoError(t, err)

		cm.AddConnection(new(MockConnection), &protobufs.AgentToServer{
			InstanceUid: serializedUUID,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "os.type",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "linux",
							},
						},
					},
				},
			},
		})

		require.Len(t, cm.connections, 1)
		assert.Equal(t, map[string]int{string(serializedUUID): 1}, cm.agentCounts)
	})

	t.Run("happy path", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		theUUID := uuid.New()
		serializedUUID, err := theUUID.MarshalText()
		require.NoError(t, err)

		cm.AddConnection(new(MockConnection), &protobufs.AgentToServer{
			InstanceUid: serializedUUID,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-abc123",
							},
						},
					},
					{
						Key: "os.type",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "linux",
							},
						},
					},
				},
			},
		})

		require.Len(t, cm.connections, 1)
		assert.Equal(t, map[string]int{"collector-abc123": 1}, cm.agentCounts)
	})
}

func TestRemoveConnection(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		theUUID := uuid.New()
		serializedUUID, err := theUUID.MarshalText()
		require.NoError(t, err)

		theConnection := new(MockConnection)
		cm.AddConnection(theConnection, &protobufs.AgentToServer{
			InstanceUid: serializedUUID,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-abc123",
							},
						},
					},
				},
			},
		})
		cm.RemoveConnection(theConnection)

		require.Len(t, cm.connections, 0)
		assert.Empty(t, cm.agentCounts)
	})
}

func TestIsAgentManaged(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		theUUID := uuid.New()
		serializedUUID, err := theUUID.MarshalText()
		require.NoError(t, err)

		theConnection := new(MockConnection)
		cm.AddConnection(theConnection, &protobufs.AgentToServer{
			InstanceUid: serializedUUID,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-abc123",
							},
						},
					},
				},
			},
		})

		assert.False(t, cm.IsAgentManaged("random-name-123"))
		assert.True(t, cm.IsAgentManaged("collector-abc123"))
	})
}

func TestDispatchRestartCommand(t *testing.T) {
	t.Run("error - sending restart", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		uuid1 := uuid.New()
		serializedUUID1, err := uuid1.MarshalText()
		require.NoError(t, err)

		uuid2 := uuid.New()
		serializedUUID2, err := uuid2.MarshalText()
		require.NoError(t, err)

		connection1 := &MockConnection{
			t:           t,
			shouldError: false,
			idToVerify:  string(serializedUUID1),
		}
		connection2 := &MockConnection{
			t:           t,
			shouldError: true,
		}

		cm.AddConnection(connection1, &protobufs.AgentToServer{
			InstanceUid: serializedUUID1,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-abc123",
							},
						},
					},
				},
			},
		})
		cm.AddConnection(connection2, &protobufs.AgentToServer{
			InstanceUid: serializedUUID2,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-xyz999",
							},
						},
					},
				},
			},
		})

		require.Error(t, cm.DispatchRestartCommand(context.Background()))
	})

	t.Run("happy path", func(t *testing.T) {
		cm := NewAgentConnectionManager(context.Background(), zaptest.NewLogger(t))

		uuid1 := uuid.New()
		serializedUUID1, err := uuid1.MarshalText()
		require.NoError(t, err)

		uuid2 := uuid.New()
		serializedUUID2, err := uuid2.MarshalText()
		require.NoError(t, err)

		connection1 := &MockConnection{
			t:           t,
			shouldError: false,
			idToVerify:  string(serializedUUID1),
		}
		connection2 := &MockConnection{
			t:           t,
			shouldError: false,
			idToVerify:  string(serializedUUID2),
		}

		cm.AddConnection(connection1, &protobufs.AgentToServer{
			InstanceUid: serializedUUID1,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-abc123",
							},
						},
					},
				},
			},
		})
		cm.AddConnection(connection2, &protobufs.AgentToServer{
			InstanceUid: serializedUUID2,
			AgentDescription: &protobufs.AgentDescription{
				NonIdentifyingAttributes: []*protobufs.KeyValue{
					{
						Key: "host.name",
						Value: &protobufs.AnyValue{
							Value: &protobufs.AnyValue_StringValue{
								StringValue: "collector-xyz999",
							},
						},
					},
				},
			},
		})

		require.NoError(t, cm.DispatchRestartCommand(context.Background()))
	})
}

// agentMessage returns an AgentToServer message for an agent with the given instance ID and host name.
func agentMessage(instanceID, hostName string) *protobufs.AgentToServer {
	return &protobufs.AgentToServer{
		InstanceUid: []byte(instanceID),
		AgentDescription: &protobufs.AgentDescription{
			NonIdentifyingAttributes: []*protobufs.KeyValue{
				{
					Key:   "host.name",
					Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: hostName}},
				},
			},
		},
	}
}

// funcConnection is a connection whose Send runs a test-provided function.
type funcConnection struct {
	send func(ctx context.Context, message *protobufs.ServerToAgent) error
}

func (*funcConnection) Connection() net.Conn { return nil }
func (*funcConnection) Disconnect() error    { return nil }
func (c *funcConnection) Send(ctx context.Context, message *protobufs.ServerToAgent) error {
	return c.send(ctx, message)
}

func TestRemoveConnection_SharedNameStaysManagedUntilLastConnection(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())
	first := new(MockConnection)
	second := new(MockConnection)
	cm.AddConnection(first, agentMessage("id-1", "collector-abc123"))
	cm.AddConnection(second, agentMessage("id-2", "collector-abc123"))

	cm.RemoveConnection(first)
	assert.True(t, cm.IsAgentManaged("collector-abc123"), "another connection still has this name")

	cm.RemoveConnection(second)
	assert.False(t, cm.IsAgentManaged("collector-abc123"))
	assert.Empty(t, cm.agentCounts)
}

func TestAddConnection_ReAddWithNewName(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())
	conn := new(MockConnection)
	cm.AddConnection(conn, agentMessage("id-1", "old-name"))

	cm.AddConnection(conn, agentMessage("id-1", "new-name"))

	assert.False(t, cm.IsAgentManaged("old-name"), "the old name has no connection left")
	assert.True(t, cm.IsAgentManaged("new-name"))
	assert.Equal(t, map[string]int{"new-name": 1}, cm.agentCounts)
	require.Len(t, cm.connections, 1)
}

func TestRemoveConnection_UnknownConnection(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())
	cm.AddConnection(new(MockConnection), agentMessage("id-1", "collector-abc123"))

	cm.RemoveConnection(new(MockConnection))

	assert.True(t, cm.IsAgentManaged("collector-abc123"))
	assert.Equal(t, map[string]int{"collector-abc123": 1}, cm.agentCounts)
}

func TestDispatchRestartCommand_ReachesEveryAgentDespiteFailures(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())

	// Order the sends so the healthy ones are still in flight when the broken one fails:
	// the broken send waits until both healthy sends have started, and they wait until it
	// has failed. They then watch their context for a while, because an implementation that
	// cancels on error does so just after the failing send returns.
	const cancelGrace = 100 * time.Millisecond
	var started sync.WaitGroup
	started.Add(2)
	failed := make(chan struct{})
	var sent atomic.Int32
	ok := func(ctx context.Context, _ *protobufs.ServerToAgent) error {
		started.Done()
		<-failed
		select {
		case <-ctx.Done():
			// A failure elsewhere must not cancel this send.
			return ctx.Err()
		case <-time.After(cancelGrace):
		}
		sent.Add(1)
		return nil
	}
	cm.AddConnection(&funcConnection{send: ok}, agentMessage("id-1", "healthy-1"))
	cm.AddConnection(&funcConnection{send: func(context.Context, *protobufs.ServerToAgent) error {
		started.Wait()
		close(failed)
		return assert.AnError
	}}, agentMessage("id-2", "broken"))
	cm.AddConnection(&funcConnection{send: ok}, agentMessage("id-3", "healthy-2"))

	err := cm.DispatchRestartCommand(t.Context())

	require.ErrorIs(t, err, assert.AnError)
	assert.Contains(t, err.Error(), "restart agent broken")
	assert.Equal(t, int32(2), sent.Load(), "the healthy agents should still be restarted")
}

func TestDispatchRestartCommand_DoesNotBlockConnectionChanges(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())

	sending := make(chan struct{})
	release := make(chan struct{})
	cm.AddConnection(&funcConnection{send: func(context.Context, *protobufs.ServerToAgent) error {
		close(sending)
		<-release
		return nil
	}}, agentMessage("id-1", "slow"))

	dispatched := make(chan error, 1)
	go func() { dispatched <- cm.DispatchRestartCommand(t.Context()) }()
	<-sending

	// While the slow send is in progress, connections can still be added and removed.
	changed := make(chan struct{})
	go func() {
		other := new(MockConnection)
		cm.AddConnection(other, agentMessage("id-2", "other"))
		cm.RemoveConnection(other)
		close(changed)
	}()
	select {
	case <-changed:
	case <-time.After(5 * time.Second):
		t.Fatal("AddConnection/RemoveConnection blocked while a restart was being sent")
	}

	close(release)
	require.NoError(t, <-dispatched)
}

func TestConnectionManager_ConcurrentUse(t *testing.T) {
	cm := NewAgentConnectionManager(t.Context(), zap.NewNop())
	noop := func(context.Context, *protobufs.ServerToAgent) error { return nil }

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			conn := &funcConnection{send: noop}
			cm.AddConnection(conn, agentMessage("id", "shared"))
			_ = cm.IsAgentManaged("shared")
			if i%2 == 0 {
				_ = cm.DispatchRestartCommand(t.Context())
			}
			cm.RemoveConnection(conn)
		})
	}
	wg.Wait()

	assert.False(t, cm.IsAgentManaged("shared"))
	assert.Empty(t, cm.agentCounts)
	assert.Empty(t, cm.connections)
}

func TestLogConnectedAgents_ReadsUnderLock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cm := NewAgentConnectionManager(ctx, zap.NewNop())

	// Log far more often than in production, while connections change; the race detector
	// flags any unsynchronized read of the connections map. (A no-op logger keeps
	// NewAgentConnectionManager from starting its own logging goroutine, which could log
	// after the test ends.)
	logged := make(chan struct{})
	go func() {
		cm.logConnectedAgents(ctx, time.Millisecond)
		close(logged)
	}()
	// Keep changing connections across many ticks, not just a few iterations.
	for deadline := time.Now().Add(50 * time.Millisecond); time.Now().Before(deadline); {
		conn := new(MockConnection)
		cm.AddConnection(conn, agentMessage("id", "collector"))
		cm.RemoveConnection(conn)
	}
	cancel()
	<-logged
}
