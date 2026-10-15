package opamp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

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
	mu          sync.RWMutex
	logger      *zap.Logger
	connections map[types.Connection]CollectorAgent
	// agentCounts counts open connections per collector name. Several connections can share
	// a name (for example while an agent reconnects), so a name stays managed until the
	// last of them is removed.
	agentCounts map[string]int
}

// connectedAgentsLogInterval is how often the number of connected agents is logged at debug level.
const connectedAgentsLogInterval = 30 * time.Second

func NewAgentConnectionManager(ctx context.Context, logger *zap.Logger) *AgentConnectionManager {
	manager := &AgentConnectionManager{
		connections: make(map[types.Connection]CollectorAgent),
		agentCounts: make(map[string]int),
		logger:      logger,
	}

	if logger.Core().Enabled(zap.DebugLevel) {
		go manager.logConnectedAgents(ctx, connectedAgentsLogInterval)
	}

	return manager
}

// logConnectedAgents logs the number of connected agents every interval until ctx is done.
func (m *AgentConnectionManager) logConnectedAgents(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.logger.Debug("AgentConnectionManager shutting down")
			return
		case <-ticker.C:
			m.mu.RLock()
			num := len(m.connections)
			m.mu.RUnlock()
			m.logger.Debug("agents connected", zap.Int("num", num))
		}
	}
}

// AddConnection adds a new connection to the list of underlying connected agents.
// Adding a connection again replaces its agent, for example if its name changed.
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
	if previous, exists := m.connections[conn]; exists {
		m.releaseAgent(previous.CollectorName)
	}
	m.connections[conn] = CollectorAgent{
		CollectorName: collectorName,
		ID:            string(message.GetInstanceUid()),
	}
	m.agentCounts[collectorName]++
}

// RemoveConnection removes the provided opamp connection from the list of underlying connected agents.
// The agent's name stays managed while other connections with the same name remain.
func (m *AgentConnectionManager) RemoveConnection(conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	agent, exists := m.connections[conn]
	if !exists {
		return
	}
	delete(m.connections, conn)
	m.releaseAgent(agent.CollectorName)
}

// releaseAgent drops one connection from name's count. The caller must hold m.mu.
func (m *AgentConnectionManager) releaseAgent(name string) {
	m.agentCounts[name]--
	if m.agentCounts[name] <= 0 {
		delete(m.agentCounts, name)
	}
}

// IsAgentManaged checks if the provided agent name is managed by the connection manager.
func (m *AgentConnectionManager) IsAgentManaged(agentName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.agentCounts[agentName] > 0
}

// DispatchRestartCommand sends the CommandType_Restart event to all connected opamp agents.
// Sends run concurrently, outside the lock, so a slow agent doesn't block connections being
// added or removed. Every agent is sent the command even if others fail; the returned error
// joins the failures.
func (m *AgentConnectionManager) DispatchRestartCommand(ctx context.Context) error {
	m.mu.RLock()
	connections := maps.Clone(m.connections)
	m.mu.RUnlock()

	var (
		wg     sync.WaitGroup
		errsMu sync.Mutex
		errs   []error
	)
	for conn, agent := range connections {
		cmd := &protobufs.ServerToAgent{
			InstanceUid: []byte(agent.ID),
			Command: &protobufs.ServerToAgentCommand{
				Type: protobufs.CommandType_CommandType_Restart,
			},
		}

		wg.Go(func() {
			m.logger.Debug("restarting agent", zap.String("name", agent.CollectorName))
			if err := conn.Send(ctx, cmd); err != nil {
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("restart agent %s: %w", agent.CollectorName, err))
				errsMu.Unlock()
			}
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}
