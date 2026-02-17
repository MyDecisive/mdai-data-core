package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/synadia-io/orbit.go/pcgroups"
	"go.uber.org/zap"

	"github.com/mydecisive/mdai-data-core/eventing"
)

const (
	connectTimeout              = 2 * time.Second
	reconnectWait               = 2 * time.Second
	flushTimeout                = 250 * time.Millisecond
	NewSubscriberContextTimeout = 5 * time.Minute
	maxPCGroupMembers           = 5

	DefaultAckWait       = 30 * time.Second
	DefaultMaxAckPending = 1 // for strict ordering, only one message will be delivered per consumer
	defaultDuplicates    = 2 * time.Minute
	initialInterval      = 250 * time.Millisecond
	maxInterval          = 60 * time.Second
	multiplier           = 2.0

	dlqSuffix = "dlq"
)

type mdaiSubjectConfig struct {
	Topic         eventing.MdaiEventType
	ConsumerGroup eventing.MdaiEventConsumerGroup
	WildcardCount int
}

func (subjectConfig mdaiSubjectConfig) validate() error {
	if subjectConfig.Topic == "" {
		return errors.New("invalid subject config, empty topic")
	}
	if subjectConfig.ConsumerGroup == "" {
		return errors.New("invalid subject config, empty consumerGroup")
	}
	return nil
}

func (subjectConfig mdaiSubjectConfig) getWildcardString() (string, error) {
	if err := subjectConfig.validate(); err != nil {
		return "", err
	}
	if subjectConfig.WildcardCount <= 0 {
		return subjectConfig.Topic.String(), nil
	}
	wildcard := strings.TrimSuffix(strings.Repeat("*.", subjectConfig.WildcardCount), ".")
	return fmt.Sprintf("%s.%s", subjectConfig.Topic, wildcard), nil
}

func (subjectConfig mdaiSubjectConfig) getPrefixedWildcardString(prefix string) (string, error) {
	wildcardedString, err := subjectConfig.getWildcardString()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s", prefix, wildcardedString), nil
}

// Used to create streams for JetStream config
func (subjectConfig mdaiSubjectConfig) getWildcardAndSuffixedSubjects(prefix string, suffixes ...string) ([]string, error) {
	subjects := make([]string, 0, len(suffixes)+1)
	prefixedString, err := subjectConfig.getPrefixedWildcardString(prefix)
	if err != nil {
		return nil, err
	}
	subjects = append(subjects, prefixedString)
	for _, suffix := range suffixes {
		subjects = append(subjects, fmt.Sprintf("%s.%s.%s", prefix, subjectConfig.Topic, suffix))
	}
	return subjects, nil
}

func (subjectConfig mdaiSubjectConfig) getWildcardIndices() []int {
	if subjectConfig.WildcardCount <= 0 {
		return []int{}
	}
	indices := make([]int, 0, subjectConfig.WildcardCount)
	for i := 1; i <= subjectConfig.WildcardCount; i++ {
		indices = append(indices, i)
	}
	return indices
}

var (
	alertSubjectConfig = mdaiSubjectConfig{
		Topic:         eventing.AlertEventType,
		ConsumerGroup: eventing.AlertConsumerGroupName,
		WildcardCount: 2,
	}
	varSubjectConfig = mdaiSubjectConfig{
		Topic:         eventing.VarEventType,
		ConsumerGroup: eventing.VarsConsumerGroupName,
		WildcardCount: 2,
	}
	triggerSubjectConfig = mdaiSubjectConfig{
		Topic:         eventing.TriggerEventType,
		ConsumerGroup: eventing.TriggerConsumerGroupName,
		WildcardCount: 3, // action, hub and key
	}
)

type allSubjectConfigs []mdaiSubjectConfig

var (
	everySubjectConfig allSubjectConfigs = []mdaiSubjectConfig{
		alertSubjectConfig,
		varSubjectConfig,
		triggerSubjectConfig,
	}
)

func (subjectConfigs allSubjectConfigs) getAllSubjectStringsWithAdditionalSuffixes(prefix string, additionalNonWildcardSuffixes ...string) ([]string, error) {
	subjects := make([]string, 0, len(additionalNonWildcardSuffixes)+1)
	for _, stream := range subjectConfigs {
		streamSubjects, err := stream.getWildcardAndSuffixedSubjects(prefix, additionalNonWildcardSuffixes...)
		if err != nil {
			return nil, err
		}
		subjects = append(subjects, streamSubjects...)
	}
	return subjects, nil
}

type Config struct {
	URL               string        `default:"nats://mdai-hub-nats.mdai.svc.cluster.local:4222" envconfig:"NATS_URL"`
	Subject           string        `default:"eventing"                                         envconfig:"NATS_SUBJECT"`
	StreamName        string        `default:"EVENTS_STREAM"                                    envconfig:"NATS_STREAM_NAME"`
	ClientName        string        `envconfig:"-"`
	InactiveThreshold time.Duration `default:"1m"                                               envconfig:"NATS_INACTIVE_THRESHOLD"`
	NatsPassword      string        `envconfig:"NATS_PASSWORD"`
	Logger            *zap.Logger   `envconfig:"-"`
	// this flag should not be used in prod environment, only for local testing and development
	DevDeleteMembers bool `default:"false"                                            envconfig:"DEV_DELETE_MEMBERS"`
}

func LoadConfig() (Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return cfg, fmt.Errorf("processing envconfig: %w", err)
	}
	return cfg, nil
}

func SafeToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return strings.NewReplacer(".", "_", " ", "_").Replace(s)
}

//nolint:ireturn
func Connect(ctx context.Context, cfg Config) (*nats.Conn, jetstream.JetStream, error) {
	natsOpts := []nats.Option{
		nats.UserInfo("mdai", cfg.NatsPassword),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.Timeout(connectTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.Name(cfg.ClientName),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			cfg.Logger.Debug("NATS disconnect", zap.Error(err))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			cfg.Logger.Debug("NATS async error", zap.Error(err))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cfg.Logger.Debug("NATS connection closed")
		}),
	}

	var conn *nats.Conn
	operation := func() (*nats.Conn, error) {
		return nats.Connect(cfg.URL, natsOpts...)
	}

	conn, err := backoff.Retry(ctx, operation)
	if err != nil {
		return nil, nil, err
	}

	// block here until we have completed an INFO/CONNECT/PONG round-trip
	waitForNATSConnection(ctx, conn, cfg)

	js, err := jetstream.New(conn) // implements pcgroups’ JetStream interface
	if err != nil {
		cfg.Logger.Error("NATS JetStream setup failed", zap.Error(err))
		_ = conn.Drain()
		return nil, nil, err
	}

	cfg.Logger.Info("NATS setup completed")
	return conn, js, nil
}

func waitForNATSConnection(ctx context.Context, conn *nats.Conn, cfg Config) {
	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = initialInterval
	exp.MaxInterval = maxInterval
	exp.Multiplier = multiplier

	notify := func(err error, next time.Duration) {
		cfg.Logger.Error(
			"NATS connection not ready, backing off",
			zap.Error(err),
			zap.Duration("next_retry_in", next),
			zap.String("nats_url", cfg.URL),
		)
	}

	operation := func() (bool, error) {
		// RetryFlush returns nil as soon as FlushTimeout succeeds.
		if err := conn.FlushTimeout(flushTimeout); err != nil {
			return false, err
		}
		cfg.Logger.Info("NATS connection verified")
		return true, nil
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithBackOff(exp),
		backoff.WithNotify(notify),
	)
	if err != nil {
		cfg.Logger.Fatal("Unable to establish NATS connection", zap.Error(err))
	}
	cfg.Logger.Info("NATS connection ready")
}

func GetMemberIDs() string {
	raw := firstNonEmpty(
		os.Getenv("POD_NAME"),
		os.Getenv("HOSTNAME"),
		nuid.Next(), // fallback for local testing
	)
	// Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '/', r == '=':
			return r
		default:
			return '_'
		}
	}, raw)

	const maxLen = 16
	if len(clean) > maxLen {
		clean = clean[len(clean)-maxLen:]
	}
	return clean
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func EnsurePCGroup(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	for _, subject := range everySubjectConfig {
		prefixedWildcardString, err := subject.getPrefixedWildcardString(cfg.Subject)
		if err != nil {
			return err
		}
		if err := ensureElasticGroup(ctx, js, cfg.StreamName, string(subject.ConsumerGroup), prefixedWildcardString, subject.getWildcardIndices(), cfg); err != nil {
			return err
		}
	}
	return nil
}

// EnsureStream ensures that the specified stream exists with the given subjects.
// It will update the stream if necessary only when new subject was added.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	desired, err := everySubjectConfig.getAllSubjectStringsWithAdditionalSuffixes(cfg.Subject, dlqSuffix)
	if err != nil {
		return err
	}
	desiredSet := mapset.NewSet[string](desired...)

	stream, err := js.Stream(ctx, cfg.StreamName)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		cfg.Logger.Info("Creating new NATS JetStream stream",
			zap.String("stream_name", cfg.StreamName),
			zap.Strings("subjects", desired))
		_, err = js.CreateStream(ctx,
			jetstream.StreamConfig{
				Name: cfg.StreamName,
				// TODO create a separate stream for DLQ since it could have different retention settings
				Subjects:   desired,
				Storage:    jetstream.FileStorage,
				Retention:  jetstream.WorkQueuePolicy, // assume no replay needed
				MaxMsgs:    -1,
				MaxBytes:   -1,
				Discard:    jetstream.DiscardOld,
				Duplicates: defaultDuplicates,
			})
		if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return err
		}
		return nil
	}
	if err != nil {
		return err // otherwise someone else just created it
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}

	// Refuse if desired is not a superset of existing.
	curSet := mapset.NewSet[string](info.Config.Subjects...)
	if !desiredSet.IsSuperset(curSet) {
		missingFromDesired := curSet.Difference(desiredSet).ToSlice()
		cfg.Logger.Error("Refusing stream update: desired subjects would drop existing subjects",
			zap.String("stream_name", cfg.StreamName),
			zap.Strings("existing_not_in_desired", missingFromDesired))
		return fmt.Errorf("desired subjects missing existing subject(s): %s", strings.Join(missingFromDesired, ", "))
	}

	// Add only truly new subjects.
	toAddSet := desiredSet.Difference(curSet)
	if toAddSet.Cardinality() == 0 {
		return nil
	}

	toAdd := toAddSet.ToSlice()
	allSubjects := desiredSet.ToSlice()
	sort.Strings(allSubjects)
	info.Config.Subjects = allSubjects // keeps all existing + new

	cfg.Logger.Info("Updating JetStream stream subjects",
		zap.String("stream_name", cfg.StreamName),
		zap.Strings("adding", toAdd),
		zap.Int("total_subjects", len(info.Config.Subjects)))

	_, err = js.UpdateStream(ctx, info.Config)
	return err
}

func ensureElasticGroup(ctx context.Context, js jetstream.JetStream, streamName, groupName, pattern string, hashWildcards []int, cfg Config) error {
	ec, _ := pcgroups.GetElasticConsumerGroupConfig(ctx, js, streamName, groupName)
	if cfg.DevDeleteMembers {
		// reset mapping, very rarely members got stuck if service is killed, this is workaround for local testing
		if ec != nil && len(ec.Members) > 1 {
			_, err := pcgroups.DeleteMembers(ctx, js, streamName, groupName, ec.Members)
			if err != nil {
				return err
			}
		}
	}

	if ec == nil {
		cfg.Logger.Info("NATS Elastic Consumer Group does not exist, creating", zap.String("group_name", groupName), zap.String("pattern", pattern))
		_, err := pcgroups.CreateElastic(
			ctx,
			js,
			streamName,
			groupName,
			maxPCGroupMembers, // works for 1-3 replicas, TODO make it configurable: partitions = replicas * 3  (rounded to something tidy, e.g. 10, 12, 16)
			pattern,
			hashWildcards,
			-1,
			-1,
		)
		if err != nil {
			cfg.Logger.Error("NATS Elastic Consumer Group creation failed", zap.Error(err))
			return err
		}
		cfg.Logger.Info("NATS Elastic Consumer Group created", zap.String("group_name", groupName), zap.String("pattern", pattern))
	}
	return nil
}
