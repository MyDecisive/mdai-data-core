package opamp

import (
	"context"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"golang.org/x/sync/errgroup"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"go.uber.org/zap"
)

// ConnectionManager is for managing connected agents for an opamp server.
type ConnectionManager interface {
	AddConnection(conn types.Connection, message *protobufs.AgentToServer)
	RemoveConnection(conn types.Connection)
	DispatchRestartCommand(ctx context.Context) error
	IsAgentManaged(agentName string) bool
}

// enforce implementation of ConnectionManager interface.
var _ ConnectionManager = &AgentConnectionManager{}

type CollectorAgent struct {
	ID            string
	CollectorName string
}

type AgentConnectionManager struct {
	mu              sync.RWMutex
	logger          *zap.Logger
	connections     map[types.Connection]CollectorAgent
	connectedAgents mapset.Set[string]
}

func NewAgentConnectionManager(ctx context.Context, logger *zap.Logger) *AgentConnectionManager {
	manager := &AgentConnectionManager{
		connections:     make(map[types.Connection]CollectorAgent),
		connectedAgents: mapset.NewThreadUnsafeSet[string](),
		logger:          logger,
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
func (m *AgentConnectionManager) AddConnection(conn types.Connection, message *protobufs.AgentToServer) {
	collectorName := string(message.GetInstanceUid())
	for _, attr := range message.GetAgentDescription().GetNonIdentifyingAttributes() {
		if attr.GetKey() == "host.name" {
			collectorName = attr.GetValue().GetStringValue()
		}
	}
	m.logger.Debug("agent connected", zap.String("name", collectorName))

	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[conn] = CollectorAgent{
		CollectorName: collectorName,
		ID:            string(message.GetInstanceUid()),
	}

	m.connectedAgents.Add(collectorName)
}

// RemoveConnection removes the provided opamp connection from the list of underlying connected agents.
func (m *AgentConnectionManager) RemoveConnection(conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for connection, agent := range m.connections {
		if connection == conn {
			m.connectedAgents.Remove(agent.CollectorName)
		}
	}
	delete(m.connections, conn)
}

// IsAgentManaged checks if the provided agent name is managed by the connection manager.
func (m *AgentConnectionManager) IsAgentManaged(agentName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connectedAgents.Contains(agentName)
}

// DispatchRestartCommand sends the CommandType_Restart event to all connected opamp agents.
func (m *AgentConnectionManager) DispatchRestartCommand(ctx context.Context) error {
	g, errCtx := errgroup.WithContext(ctx)

	m.mu.RLock()
	defer m.mu.RUnlock()
	for conn, agent := range m.connections {
		cmd := &protobufs.ServerToAgent{
			InstanceUid: []byte(agent.ID),
			Command: &protobufs.ServerToAgentCommand{
				Type: protobufs.CommandType_CommandType_Restart,
			},
		}

		g.Go(func() error {
			m.logger.Debug("restarting agent", zap.String("name", agent.CollectorName))
			return conn.Send(errCtx, cmd)
		})
	}

	return g.Wait()
}
