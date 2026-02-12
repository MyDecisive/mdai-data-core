package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

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
	deregisterContextTimeout    = 60 * time.Second
)

type Subscriber interface {
	Subscribe(ctx context.Context, groupName, dlqSubject string, invoker eventing.HandlerInvoker) error
	Close() error
}

type EventSubscriber struct {
	cfg       config.Config
	logger    *zap.Logger
	conn      *nats.Conn
	jetStream jetstream.JetStream

	waitGroup sync.WaitGroup
	closeOnce sync.Once

	memberID string

	joinedGroups []string // assuming no duplicates subscriptions created

	consumers map[string]pcgroups.ConsumerGroupConsumeContext
	mu        sync.Mutex
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
	}
	return s, nil
}

// Subscribe to a NATS subject and handle incoming messages.
// process incoming messages using the provided handler in strict order
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

	consumerConfig := jetstream.ConsumerConfig{
		AckWait:       config.DefaultAckWait,
		MaxAckPending: config.DefaultMaxAckPending,
		// Durable:       s.cfg.DurableName,
		AckPolicy:         jetstream.AckExplicitPolicy,
		InactiveThreshold: s.cfg.InactiveThreshold,
	}

	ec, err := pcgroups.GetElasticConsumerGroupConfig(ctx, s.jetStream, s.cfg.StreamName, groupName) // TODO add retry?
	if err != nil {
		return fmt.Errorf("get Elastic Consumer Group config (%s): %w", groupName, err)
	}

	memberID := s.memberID
	if !ec.IsInMembership(memberID) {
		members, err := pcgroups.AddMembers(ctx, s.jetStream, s.cfg.StreamName, groupName, []string{memberID})
		if err != nil {
			return fmt.Errorf("add member to %s: %w", groupName, err)
		}
		s.logger.Info("Elastic group membership added",
			zap.String("group", groupName),
			zap.String("memberID", memberID),
			zap.Reflect("members", members),
		)
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
		return fmt.Errorf("ElasticConsume(%s): %w", groupName, err)
	}

	s.mu.Lock()
	s.consumers[groupName] = cons
	s.joinedGroups = append(s.joinedGroups, groupName)
	s.mu.Unlock()

	s.logger.Info("Consumer started",
		zap.String("group", groupName),
		zap.String("prefix", s.cfg.Subject),
	)

	return nil
}

func (s *EventSubscriber) Close() error {
	var err error
	s.closeOnce.Do(func() {
		// Stop consume instances (this just cancels their contexts)
		s.mu.Lock()
		for name, c := range s.consumers {
			if c != nil {
				c.Stop()
				s.logger.Info("consumer stopped", zap.String("group", name))
			}
		}
		s.mu.Unlock()

		// Wait for in-flight handlers to finish their work/acks
		s.waitGroup.Wait()

		// Deregister from each elastic group we joined
		deregisterCtx, cancel := context.WithTimeout(context.Background(), deregisterContextTimeout)
		defer cancel()
		for _, groupName := range s.joinedGroups {
			members, dropErr := pcgroups.DeleteMembers(
				deregisterCtx,
				s.jetStream,
				s.cfg.StreamName,
				groupName,
				[]string{s.memberID},
			)
			if dropErr != nil {
				s.logger.Error("failed to deregister from elastic group",
					zap.String("group", groupName),
					zap.Error(dropErr),
					zap.String("memberID", s.memberID),
					zap.Strings("members", members),
				)
			} else {
				s.logger.Info("deregistered from elastic group",
					zap.String("group", groupName),
					zap.String("memberID", s.memberID),
					zap.Strings("members", members),
				)
			}
		}

		if s.conn != nil && !s.conn.IsClosed() {
			err = s.conn.Drain()
		}
	})
	return err
}

func (s *EventSubscriber) handleMessage(ctx context.Context, msg jetstream.Msg, dlqSubject string, invoker eventing.HandlerInvoker) {
	s.waitGroup.Add(1)
	defer s.waitGroup.Done()

	if metadata, _ := msg.Metadata(); metadata != nil {
		s.logger.Info("delivery attempt",
			zap.Uint64("consumer_seq", metadata.Sequence.Consumer),
			zap.Uint64("stream_seq", metadata.Sequence.Stream))
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
		if forwardToDLQ("json_unmarshal_error", err) {
			_ = msg.Ack()
		} else {
			_ = msg.Nak()
		}
		return
	}

	if err := invoker(event); err != nil {
		s.logger.Error("handler", zap.Error(err))
		if forwardToDLQ("handler_error", err) {
			_ = msg.Ack()
		} else {
			_ = msg.Nak()
		}
		return
	}
	_ = msg.Ack()
}

var _ Subscriber = (*EventSubscriber)(nil)
