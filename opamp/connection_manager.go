package opamp

import (
	"context"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server/types"
	"sync"
	"time"
)

type ConnectionManager struct {
	mu          sync.RWMutex
	connections map[types.Connection]string // Keyed by the connection to the instanceID
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[types.Connection]string),
	}
}

func (m *ConnectionManager) Add(id string, conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections[conn] = id
}

func (m *ConnectionManager) Remove(conn types.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.connections, conn)
}

func (m *ConnectionManager) SendRestartCommand(ctx context.Context) error {
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
