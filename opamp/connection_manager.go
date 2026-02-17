package opamp

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
)

// ConnectionManager is for managing connected agents for an opamp server.
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

	if logger.Level() == zap.DebugLevel {
		connectedAgentsTimer := time.Tick(30 * time.Second)
		go func() {
			for {
				select {
				case <-ctx.Done():
					logger.Debug("AgentConnectionManager shutting down")
					return
				case <-connectedAgentsTimer:
					logger.Debug("agents connected", zap.Int("num", len(manager.connections)))
				}
			}
		}()
	}

	return manager
}

// AddConnection adds a new connection to the list of underlying connected agents.
func (m *AgentConnectionManager) AddConnection(conn types.Connection, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[conn] = id
}

// RemoveConnection removes the provided opamp connection from the list of underlying connected agents.
func (m *AgentConnectionManager) RemoveConnection(conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.connections, conn)
}

// DispatchRestartCommand sends the CommandType_Restart event to all connected opamp agents.
func (m *AgentConnectionManager) DispatchRestartCommand(ctx context.Context) error {
	g, errCtx := errgroup.WithContext(ctx)

	m.mu.RLock()
	defer m.mu.RUnlock()
	for conn, id := range m.connections {
		cmd := &protobufs.ServerToAgent{
			InstanceUid: []byte(id),
			Command: &protobufs.ServerToAgentCommand{
				Type: protobufs.CommandType_CommandType_Restart,
			},
		}

		g.Go(func() error {
			return conn.Send(errCtx, cmd)
		})
	}

	return g.Wait()
}
