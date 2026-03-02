package opamp

import (
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		require.Len(t, cm.connectedAgents.ToSlice(), 1)
		assert.Equal(t, string(serializedUUID), cm.connectedAgents.ToSlice()[0])
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
		require.Len(t, cm.connectedAgents.ToSlice(), 1)
		assert.Equal(t, "collector-abc123", cm.connectedAgents.ToSlice()[0])
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
		require.Len(t, cm.connectedAgents.ToSlice(), 0)
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
