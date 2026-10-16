package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/eventing/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

//go:generate mockgen -source=publisher.go -destination=../../internal/mocks/eventing/publisher/publisher.go -package=publisher
type Publisher interface {
	Publish(ctx context.Context, event eventing.MdaiEvent, subject eventing.MdaiEventSubject) error
	Close() error
}

type EventPublisher struct {
	cfg    config.Config
	logger *zap.Logger
	conn   *nats.Conn
	js     jetstream.JetStream
}

func NewPublisher(ctx context.Context, logger *zap.Logger, clientName string) (*EventPublisher, error) {
	logger.Info("Initializing NATS publisher", zap.String("client_name", clientName))
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}

	cfg.Logger = logger
	cfg.ClientName = clientName

	ctx, cancel := context.WithTimeout(ctx, config.NewSubscriberContextTimeout)
	defer cancel()

	conn, js, err := config.Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	cfg.Logger.Info("Publisher connected to NATS", zap.String("nats_url", cfg.URL))

	// setup only stream from publisher, keep consumer group creation in subscribers
	if err := config.EnsureStream(ctx, js, cfg); err != nil {
		_ = conn.Drain()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	return &EventPublisher{cfg: cfg, logger: cfg.Logger, conn: conn, js: js}, nil
}

func (p *EventPublisher) Publish(ctx context.Context, event eventing.MdaiEvent, subject eventing.MdaiEventSubject) error {
	event.ApplyDefaults() // TODO this should happen at event creation time

	if subject.Type == "" {
		return errors.New("subject is required")
	}
	if subject.Path == "" {
		return errors.New("subject path is required")
	}

	fullSubject := subject.PrefixedString(p.cfg.Subject)
	// Log metadata only: the payload can carry variable values and other sensitive data.
	p.logger.Info("Publishing event to subject",
		zap.String("subject", fullSubject),
		zap.String("event_id", event.ID),
		zap.String("event_name", event.Name),
		zap.String("source", event.Source),
		zap.String("hub_name", event.HubName),
		zap.String("correlation_id", event.CorrelationID),
		zap.Int("recursion_depth", event.RecursionDepth),
		zap.Int("payload_bytes", len(event.Payload)),
	)

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: fullSubject,
		Data:    data,
		Header: nats.Header{
			"name":        []string{event.Name}, // TODO review headers = identity, tracing, safety, and schema hints
			"source":      []string{event.Source},
			"hubName":     []string{event.HubName},
			nats.MsgIdHdr: []string{event.ID},
		},
	}
	if event.CorrelationID != "" {
		msg.Header.Set("correlationId", event.CorrelationID)
	}

	pubAck, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return err
	}
	p.logger.Debug("Published message",
		zap.String("subject", fullSubject),
		zap.String("event_id", event.ID),
		zap.String("stream", pubAck.Stream),
		zap.Uint64("sequence", pubAck.Sequence),
		zap.Bool("duplicate", pubAck.Duplicate),
	)
	return nil
}

func (p *EventPublisher) Close() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return p.conn.Drain()
	}
	return nil
}

var _ Publisher = (*EventPublisher)(nil)
