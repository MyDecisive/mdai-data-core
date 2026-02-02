package opamp

import (
	"context"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
	"sync"
	"time"
)

// TODO: mockery generate mock

type ConnectionManager interface {
	AddConnection(conn types.Connection, id string)
	RemoveConnection(conn types.Connection)
	DispatchRestartCommand(ctx context.Context) error
}

// enforce implementation of ConnectionManager interface.
var _ ConnectionManager = &AgentConnectionManager{}

type AgentConnectionManager struct {
	mu          sync.RWMutex
	connections map[types.Connection]string // Keyed by the connection to the instanceID
}

func NewAgentConnectionManager(ctx context.Context, logger *zap.Logger) *AgentConnectionManager {
	manager := &AgentConnectionManager{
		connections: make(map[types.Connection]string),
	}

	go func() {
		select {
		case <-ctx.Done():
			logger.Info("AgentConnectionManager shutting down")
			return
		case <-time.After(30 * time.Second):
			logger.Info("agents connected", zap.Int("num", len(manager.connections)))
		}
	}()

	return manager
}

func (m *AgentConnectionManager) AddConnection(conn types.Connection, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[conn] = id
}

func (m *AgentConnectionManager) RemoveConnection(conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.connections, conn)
}

func (m *AgentConnectionManager) DispatchRestartCommand(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	for conn, id := range m.connections {
		cmd := &protobufs.ServerToAgent{
			InstanceUid: []byte(id),
			Command: &protobufs.ServerToAgentCommand{
				Type: protobufs.CommandType_CommandType_Restart,
			},
		}

		if err := conn.Send(timeoutCtx, cmd); err != nil {
			return err
		}
	}
	return nil
}
