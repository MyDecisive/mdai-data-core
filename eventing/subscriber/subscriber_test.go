package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

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

	err = sub.Subscribe(t.Context(), "wrong_consumer_group", "alert", func(ev eventing.MdaiEvent) error {
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
	err = sub.Subscribe(ctx, "", "alert", validInvoker)
	require.EqualError(t, err, "groupName is required")

	// invoker required
	err = sub.Subscribe(ctx, eventing.AlertConsumerGroupName.String(), "alert", nil)
	require.EqualError(t, err, "invoker is required")

	// dlqSubject required
	err = sub.Subscribe(ctx, eventing.AlertConsumerGroupName.String(), "", validInvoker)
	require.EqualError(t, err, "dlqSubject is required")
}

func TestSubscriber_SubscribeSuccessfully(t *testing.T) {
	srv := runJetStream(t)

	logger := zap.NewNop()

	sub, err := NewSubscriber(context.Background(), logger, "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	received := make(chan eventing.MdaiEvent, 1)
	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(ev eventing.MdaiEvent) error {
		received <- ev
		return nil
	})
	require.NoError(t, err)

	publishEvent(t, srv, eventing.MdaiEvent{Name: "Hello", HubName: "hub", Source: "test", Payload: `{}`})

	select {
	case ev := <-received:
		assert.Equal(t, "Hello", ev.Name)
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not receive the published event")
	}
}

// publishEvent publishes ev on an alert subject that the alert consumer group consumes.
func publishEvent(t *testing.T, srv *server.Server, ev eventing.MdaiEvent) {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	data, err := json.Marshal(ev)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = js.Publish(ctx, "eventing.alert.hub.test", data)
	require.NoError(t, err)
}

// subscribeDLQ returns a core NATS subscription to the alert dead-letter subject.
func subscribeDLQ(t *testing.T, srv *server.Server) *nats.Subscription {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	dlq, err := nc.SubscribeSync("eventing.alert.dlq")
	require.NoError(t, err)
	return dlq
}

func TestSubscribe_RejectsUnknownDLQSubject(t *testing.T) {
	_ = runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alerts", func(eventing.MdaiEvent) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), `dlqSubject "alerts"`)
	assert.Contains(t, err.Error(), "alert, var, trigger.vars")
}

func TestSubscribe_RejectsDuplicateGroup(t *testing.T) {
	_ = runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	noop := func(eventing.MdaiEvent) error { return nil }
	require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", noop))

	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", noop)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestSubscribe_AfterClose(t *testing.T) {
	_ = runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	require.NoError(t, sub.Close())

	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error { return nil })
	require.ErrorIs(t, err, errSubscriberClosed)
}

func TestSubscriber_HandlerPanicGoesToDLQ(t *testing.T) {
	srv := runJetStream(t)
	dlq := subscribeDLQ(t, srv)

	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error {
		panic("boom")
	})
	require.NoError(t, err)

	publishEvent(t, srv, eventing.MdaiEvent{Name: "Panics", HubName: "hub", Source: "test", Payload: `{}`})

	msg, err := dlq.NextMsg(10 * time.Second)
	require.NoError(t, err, "the panicking handler's message should be forwarded to the DLQ")
	assert.Equal(t, "handler_panic", msg.Header.Get("dlq_reason"))
	assert.Contains(t, msg.Header.Get("dlq_error"), "boom")
}

func TestClose_WaitsForInFlightHandler(t *testing.T) {
	srv := runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	err = sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error {
		close(started)
		<-release
		finished.Store(true)
		return nil
	})
	require.NoError(t, err)

	publishEvent(t, srv, eventing.MdaiEvent{Name: "Slow", HubName: "hub", Source: "test", Payload: `{}`})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()

	select {
	case <-closed:
		t.Fatal("Close returned while a handler was still running")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case <-closed:
		assert.True(t, finished.Load(), "Close should return only after the handler finished")
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the handler finished")
	}
}

func TestClose_BoundedWhenServerIsGone(t *testing.T) {
	srv := runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	sub.deregisterTimeout = 500 * time.Millisecond
	require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error { return nil }))

	srv.Shutdown()
	srv.WaitForShutdown()
	require.Eventually(t, func() bool { return !sub.conn.IsConnected() }, 5*time.Second, 10*time.Millisecond)

	start := time.Now()
	_ = sub.Close() // Drain reports the connection is not usable; that's expected here.
	assert.Less(t, time.Since(start), 3*time.Second, "Close should give up when the deregistration budget runs out")
}

// startServerAt starts a JetStream server on a fixed port and store directory, so a test can
// restart it with the same address and state.
func startServerAt(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{JetStream: true, StoreDir: storeDir, Port: port})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats-server did not start")
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns
}

// isMember reports whether memberID is in the alert group's stored membership.
func isMember(t *testing.T, srv *server.Server, memberID string) bool {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ec, err := pcgroups.GetElasticConsumerGroupConfig(t.Context(), js, "EVENTS_STREAM", eventing.AlertConsumerGroupName.String())
	require.NoError(t, err)
	return ec.IsInMembership(memberID)
}

func TestClose_LeavesGroupAfterReconnect(t *testing.T) {
	first := runJetStream(t)
	port := first.Addr().(*net.TCPAddr).Port
	storeDir := first.StoreDir()

	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)
	require.NoError(t, sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error { return nil }))
	require.True(t, isMember(t, first, sub.memberID))

	// A temporary outage: the server goes away and comes back with the same state while
	// Close is running. Close must wait for the reconnect instead of skipping cleanup.
	first.Shutdown()
	first.WaitForShutdown()
	require.Eventually(t, func() bool { return !sub.conn.IsConnected() }, 5*time.Second, 10*time.Millisecond)

	restarted := make(chan *server.Server, 1)
	time.AfterFunc(300*time.Millisecond, func() { restarted <- startServerAt(t, port, storeDir) })

	_ = sub.Close()

	second := <-restarted
	assert.False(t, isMember(t, second, sub.memberID), "Close should remove the member once reconnected")
}

func TestClose_WaitsForSubscribeStartup(t *testing.T) {
	srv := runJetStream(t)
	sub, err := NewSubscriber(context.Background(), zap.NewNop(), "test-client")
	require.NoError(t, err)

	// Pause Subscribe right after it joins the group, then start Close.
	joined := make(chan struct{})
	release := make(chan struct{})
	sub.afterJoin = func(string) {
		close(joined)
		<-release
	}

	subscribed := make(chan error, 1)
	go func() {
		subscribed <- sub.Subscribe(t.Context(), eventing.AlertConsumerGroupName.String(), "alert", func(eventing.MdaiEvent) error { return nil })
	}()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe did not join the group")
	}

	closed := make(chan error, 1)
	go func() { closed <- sub.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while Subscribe was still starting")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	require.ErrorIs(t, <-subscribed, errSubscriberClosed)
	select {
	case <-closed:
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return after Subscribe finished")
	}
	assert.False(t, isMember(t, srv, sub.memberID), "Close should leave the group Subscribe joined")
}

func TestDLQRetryDelay(t *testing.T) {
	cases := map[uint64]time.Duration{
		0:   time.Second,
		1:   time.Second,
		2:   2 * time.Second,
		3:   4 * time.Second,
		5:   16 * time.Second,
		6:   30 * time.Second, // 32s capped
		100: 30 * time.Second,
	}
	for numDelivered, want := range cases {
		assert.Equal(t, want, dlqRetryDelay(numDelivered), "numDelivered=%d", numDelivered)
	}
}

// fakeMsg records how a message was settled. Methods not overridden panic if called.
type fakeMsg struct {
	jetstream.Msg
	data         []byte
	numDelivered uint64

	acked        bool
	naked        bool
	nakDelay     time.Duration
	nakWithDelay bool
}

func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Headers() nats.Header { return nats.Header{} }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeMsg) Ack() error { m.acked = true; return nil }
func (m *fakeMsg) Nak() error { m.naked = true; return nil }
func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	m.nakWithDelay = true
	m.nakDelay = delay
	return nil
}

// failingJetStream fails every publish, as when the DLQ cannot be reached.
type failingJetStream struct {
	jetstream.JetStream
}

func (failingJetStream) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, errors.New("DLQ unavailable")
}

func TestHandleMessage_DLQFailureNaksWithDelay(t *testing.T) {
	s := &EventSubscriber{logger: zap.NewNop(), jetStream: failingJetStream{}, consumers: map[string]pcgroups.ConsumerGroupConsumeContext{}}
	data, err := json.Marshal(eventing.MdaiEvent{Name: "Fails", HubName: "hub", Payload: `{}`})
	require.NoError(t, err)
	msg := &fakeMsg{data: data, numDelivered: 3}

	s.handleMessage(t.Context(), msg, "eventing.alert.dlq", func(eventing.MdaiEvent) error {
		return errors.New("handler failed")
	})

	assert.False(t, msg.acked, "a message that could not be dead-lettered must not be acked")
	assert.False(t, msg.naked, "it should not be redelivered immediately")
	assert.True(t, msg.nakWithDelay)
	assert.Equal(t, 4*time.Second, msg.nakDelay)
}

func TestHandleMessage_NaksWithoutHandlingWhenClosing(t *testing.T) {
	s := &EventSubscriber{logger: zap.NewNop(), consumers: map[string]pcgroups.ConsumerGroupConsumeContext{}}
	s.closing = true
	msg := &fakeMsg{data: []byte(`{}`)}

	called := false
	s.handleMessage(t.Context(), msg, "eventing.alert.dlq", func(eventing.MdaiEvent) error {
		called = true
		return nil
	})

	assert.False(t, called, "no new handler should start once Close has begun")
	assert.True(t, msg.naked, "the message should be handed back for redelivery")
	assert.False(t, msg.acked)
}
