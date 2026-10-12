package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/eventing/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"
)

const (
	dlqSuffix                   = ".dlq"
	newSubscriberContextTimeout = 5 * time.Minute
	subscribeContextTimeout     = 10 * time.Second
	deregisterContextTimeout    = 10 * time.Second

	// Redelivery delay after a failed DLQ publish: doubles per delivery attempt, up to the max.
	dlqRetryInitialDelay = time.Second
	dlqRetryMaxDelay     = 30 * time.Second
)

var errSubscriberClosed = errors.New("subscriber is closed")

type Subscriber interface {
	Subscribe(ctx context.Context, groupName, dlqSubject string, invoker eventing.HandlerInvoker) error
	Close() error
}

type EventSubscriber struct {
	cfg       config.Config
	logger    *zap.Logger
	conn      *nats.Conn
	jetStream jetstream.JetStream

	waitGroup sync.WaitGroup // in-flight message handlers
	closeOnce sync.Once

	memberID string

	// deregisterTimeout bounds how long Close spends leaving elastic groups, including
	// waiting for a reconnect.
	deregisterTimeout time.Duration
	// afterJoin, if set, runs right after a group's membership is confirmed. Tests only.
	afterJoin func(groupName string)

	mu sync.Mutex
	// consumers holds one entry per subscribed group. A nil value reserves the group while
	// Subscribe is still starting its consumer.
	consumers map[string]pcgroups.ConsumerGroupConsumeContext
	// joined holds every group this member has joined, including any whose Subscribe later
	// failed or lost a race with Close. Close leaves all of them.
	joined map[string]struct{}
	// starting counts Subscribe calls between reserving a group and registering its consumer.
	starting sync.WaitGroup
	// closing is set by Close; once set, no new handler starts and Subscribe fails.
	closing bool
}

func NewSubscriber(ctx context.Context, logger *zap.Logger, clientName string) (*EventSubscriber, error) {
	logger.Info("Initializing NATS subscriber", zap.String("client_name", clientName))
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}

	cfg.Logger = logger
	cfg.ClientName = clientName

	handshakeCtx, cancel := context.WithTimeout(ctx, newSubscriberContextTimeout)
	defer cancel()

	conn, js, err := config.Connect(handshakeCtx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	if err := config.EnsureStream(handshakeCtx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	if err := config.EnsurePCGroup(handshakeCtx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure pcgroup: %w", err)
	}

	s := &EventSubscriber{
		cfg:       cfg,
		logger:    cfg.Logger,
		conn:      conn,
		jetStream: js,
		memberID:  config.GetMemberIDs(),
		consumers: make(map[string]pcgroups.ConsumerGroupConsumeContext),
		joined:    make(map[string]struct{}),

		deregisterTimeout: deregisterContextTimeout,
	}
	return s, nil
}

// Subscribe to a NATS subject and handle incoming messages.
// process incoming messages using the provided handler in strict order
// dlqSubject must be one of config.DLQTopics. A group can be subscribed to only once per
// subscriber, and Subscribe fails after Close.
func (s *EventSubscriber) Subscribe(ctx context.Context, groupName string, dlqSubject string, invoker eventing.HandlerInvoker) error {
	if groupName == "" {
		return errors.New("groupName is required")
	}
	if invoker == nil {
		return errors.New("invoker is required")
	}
	if dlqSubject == "" {
		return errors.New("dlqSubject is required")
	}
	if !slices.Contains(config.DLQTopics(), dlqSubject) {
		return fmt.Errorf("dlqSubject %q has no dead-letter subject in the stream (valid: %s)",
			dlqSubject, strings.Join(config.DLQTopics(), ", "))
	}

	if err := s.reserveGroup(groupName); err != nil {
		return err
	}
	defer s.starting.Done()

	cons, err := s.consume(ctx, groupName, dlqSubject, invoker)
	if err != nil {
		s.releaseGroup(groupName)
		return err
	}

	s.mu.Lock()
	if s.closing {
		// Close began while the consumer was starting and is waiting for this call to return.
		// Stop the consumer here; the group stays in joined, so Close still leaves it.
		delete(s.consumers, groupName)
		s.mu.Unlock()
		cons.Stop()
		return errSubscriberClosed
	}
	s.consumers[groupName] = cons
	s.mu.Unlock()

	s.logger.Info("Consumer started",
		zap.String("group", groupName),
		zap.String("prefix", s.cfg.Subject),
	)

	return nil
}

// reserveGroup claims groupName for a new subscription, failing if it is already subscribed
// (or being subscribed) or the subscriber is closed. On success the caller must call
// s.starting.Done once the subscription is registered or abandoned. Adding to starting under
// the lock, only while not closing, means Close's Wait never races with Add.
func (s *EventSubscriber) reserveGroup(groupName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return errSubscriberClosed
	}
	if _, exists := s.consumers[groupName]; exists {
		return fmt.Errorf("already subscribed to group %q", groupName)
	}
	s.consumers[groupName] = nil
	s.starting.Add(1)
	return nil
}

// markJoined records that this member belongs to groupName, so Close leaves it.
func (s *EventSubscriber) markJoined(groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joined[groupName] = struct{}{}
}

// releaseGroup drops a reservation made by reserveGroup after Subscribe failed.
func (s *EventSubscriber) releaseGroup(groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumers, groupName)
}

// consume joins the elastic group and starts consuming its messages.
func (s *EventSubscriber) consume(ctx context.Context, groupName string, dlqSubject string, invoker eventing.HandlerInvoker) (pcgroups.ConsumerGroupConsumeContext, error) {
	consumerConfig := jetstream.ConsumerConfig{
		AckWait:       config.DefaultAckWait,
		MaxAckPending: config.DefaultMaxAckPending,
		// Durable:       s.cfg.DurableName,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: s.cfg.InactiveThreshold,
	}

	ec, err := pcgroups.GetElasticConsumerGroupConfig(ctx, s.jetStream, s.cfg.StreamName, groupName) // TODO add retry?
	if err != nil {
		return nil, fmt.Errorf("get Elastic Consumer Group config (%s): %w", groupName, err)
	}

	memberID := s.memberID
	if !ec.IsInMembership(memberID) {
		members, err := pcgroups.AddMembers(ctx, s.jetStream, s.cfg.StreamName, groupName, []string{memberID})
		if err != nil {
			return nil, fmt.Errorf("add member to %s: %w", groupName, err)
		}
		s.logger.Info("Elastic group membership added",
			zap.String("group", groupName),
			zap.String("memberID", memberID),
			zap.Reflect("members", members),
		)
	}
	s.markJoined(groupName)
	if s.afterJoin != nil {
		s.afterJoin(groupName)
	}

	fullDlqSubject := s.cfg.Subject + "." + dlqSubject + dlqSuffix
	handler := func(msg jetstream.Msg) {
		s.handleMessage(ctx, msg, fullDlqSubject, invoker)
	}

	cons, err := pcgroups.ElasticConsume(
		ctx,
		s.jetStream, s.cfg.StreamName, groupName, s.memberID, handler, consumerConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("ElasticConsume(%s): %w", groupName, err)
	}
	return cons, nil
}

// Close stops all consumers, waits for in-flight handlers, leaves the elastic groups and
// drains the connection. Messages delivered after Close starts are Nak'd for redelivery.
// Subscribe calls still starting are allowed to finish first, so no joined group is missed.
// Leaving the groups is retried, waiting for a reconnect if needed, for up to
// deregisterContextTimeout; if that fails, the membership stays behind and is logged.
func (s *EventSubscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		// Stop new handlers and subscriptions from starting.
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()

		// Let Subscribe calls already starting finish: they see closing, stop their consumers
		// and return, and any group they joined is in s.joined by then.
		s.starting.Wait()

		s.mu.Lock()
		consumers := maps.Clone(s.consumers)
		joined := slices.Sorted(maps.Keys(s.joined))
		s.mu.Unlock()

		// Stop consume instances (this just cancels their contexts)
		for name, c := range consumers {
			if c != nil {
				c.Stop()
				s.logger.Info("consumer stopped", zap.String("group", name))
			}
		}

		// Wait for in-flight handlers to finish their work/acks
		s.waitGroup.Wait()

		s.leaveGroups(joined)

		if s.conn != nil && !s.conn.IsClosed() {
			err = s.conn.Drain()
		}
	})
	return err
}

// leaveGroups removes this member from each elastic group, so its partitions are reassigned.
// Rejoining does not clean up a stale member (AddMembers keeps existing members), so this is
// the only cleanup. Each removal is retried until it succeeds or deregisterTimeout runs out,
// which also covers waiting for a reconnect. It gives up only if the connection is closed.
func (s *EventSubscriber) leaveGroups(groupNames []string) {
	if len(groupNames) == 0 {
		return
	}
	if s.conn == nil || s.conn.IsClosed() {
		s.logger.Error("NATS connection closed; elastic group membership left behind",
			zap.Strings("groups", groupNames), zap.String("memberID", s.memberID))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.deregisterTimeout)
	defer cancel()
	for _, groupName := range groupNames {
		s.leaveGroup(ctx, groupName)
	}
}

func (s *EventSubscriber) leaveGroup(ctx context.Context, groupName string) {
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = 100 * time.Millisecond
	retry.MaxInterval = time.Second

	members, err := backoff.Retry(ctx, func() ([]string, error) {
		return pcgroups.DeleteMembers(ctx, s.jetStream, s.cfg.StreamName, groupName, []string{s.memberID})
	}, backoff.WithBackOff(retry))
	if err != nil {
		s.logger.Error("failed to deregister from elastic group; membership left behind",
			zap.String("group", groupName),
			zap.Error(err),
			zap.String("memberID", s.memberID),
		)
		return
	}
	s.logger.Info("deregistered from elastic group",
		zap.String("group", groupName),
		zap.String("memberID", s.memberID),
		zap.Strings("members", members),
	)
}

// beginHandling registers an in-flight handler, or returns false once Close has started.
// Registering under the lock means Close's Wait never races with Add.
func (s *EventSubscriber) beginHandling() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.waitGroup.Add(1)
	return true
}

// dlqRetryDelay is how long to wait before redelivering a message whose DLQ publish failed,
// given how many times it has been delivered: 1s, 2s, 4s, ... up to dlqRetryMaxDelay.
func dlqRetryDelay(numDelivered uint64) time.Duration {
	delay := dlqRetryInitialDelay
	for i := uint64(1); i < numDelivered && delay < dlqRetryMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, dlqRetryMaxDelay)
}

// invokeSafely calls invoker, turning a panic into an error so one bad event can't crash
// the service; the message then goes to the DLQ like any other handler error.
func invokeSafely(invoker eventing.HandlerInvoker, event eventing.MdaiEvent) (panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return false, invoker(event)
}

func (s *EventSubscriber) handleMessage(ctx context.Context, msg jetstream.Msg, dlqSubject string, invoker eventing.HandlerInvoker) {
	if !s.beginHandling() {
		// Closing: hand the message back for redelivery instead of starting new work.
		_ = msg.Nak()
		return
	}
	defer s.waitGroup.Done()

	numDelivered := uint64(1)
	if metadata, _ := msg.Metadata(); metadata != nil {
		numDelivered = metadata.NumDelivered
		s.logger.Info("delivery attempt",
			zap.Uint64("consumer_seq", metadata.Sequence.Consumer),
			zap.Uint64("stream_seq", metadata.Sequence.Stream),
			zap.Uint64("num_delivered", metadata.NumDelivered))
	}

	// settleFailed acks a failed message once it is in the DLQ. If the DLQ publish failed, it
	// Naks with a growing delay instead of an immediate redelivery, so the partition (one
	// message in flight at a time) retries at a steady pace rather than in a tight loop.
	settleFailed := func(forwarded bool) {
		if forwarded {
			_ = msg.Ack()
			return
		}
		_ = msg.NakWithDelay(dlqRetryDelay(numDelivered))
	}

	forwardToDLQ := func(reason string, err error) bool {
		// copy headers so we don't mutate the in-flight message
		header := nats.Header{}
		for k, vv := range msg.Headers() {
			header[k] = append([]string(nil), vv...)
		}
		header.Set("dlq_reason", reason)
		header.Set("dlq_error", err.Error())

		dlq := &nats.Msg{
			Subject: dlqSubject,
			Data:    msg.Data(),
			Header:  header,
		}

		ctxDLQ, cancel := context.WithTimeout(ctx, subscribeContextTimeout)
		defer cancel()
		if _, pubErr := s.jetStream.PublishMsg(ctxDLQ, dlq); pubErr != nil {
			s.logger.Error("publish DLQ failed", zap.Error(pubErr), zap.String("dlq_subject", dlqSubject))
			return false
		}

		s.logger.Warn("sent message to DLQ",
			zap.String("dlq_subject", dlqSubject),
			zap.String("reason", reason),
			zap.Int("bytes", len(msg.Data())))
		return true
	}

	var event eventing.MdaiEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		s.logger.Error("unmarshal", zap.Error(err))
		settleFailed(forwardToDLQ("json_unmarshal_error", err))
		return
	}

	if panicked, err := invokeSafely(invoker, event); err != nil {
		reason := "handler_error"
		if panicked {
			reason = "handler_panic"
			s.logger.Error("handler panicked", zap.Error(err), zap.Stack("stack"))
		} else {
			s.logger.Error("handler", zap.Error(err))
		}
		settleFailed(forwardToDLQ(reason, err))
		return
	}
	_ = msg.Ack()
}

var _ Subscriber = (*EventSubscriber)(nil)
