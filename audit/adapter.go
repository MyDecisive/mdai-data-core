package audit

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/prometheus/alertmanager/template"
	"github.com/valkey-io/valkey-go"
)

var (
	metricRegex = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\{`)

	// ErrInvalidRetention is returned when an audit stream retention environment variable cannot be parsed.
	ErrInvalidRetention = errors.New("invalid audit stream retention")
)

const (
	HubName          = "hub_name"
	Expression       = "expression"
	CurrentValue     = "current_value"
	AlertName        = "alert_name"
	EventTriggered   = "event_triggered"
	VariableUpdated  = "variable_updated"
	CollectorRestart = "collector_restart"
	ValkeyUpdate     = "valkey_update"
	Evaluation       = "evaluation"

	MdaiHubEventHistoryStreamName = "mdai_hub_event_history"

	envRetention      = "VALKEY_AUDIT_STREAM_RETENTION" // e.g. "30d", "72h", "2592000000ms"
	envRetentionMsOld = "VALKEY_AUDIT_STREAM_EXPIRY_MS" // deprecated
	defaultRetention  = 30 * 24 * time.Hour
)

type AuditAdapter struct {
	logger                  *zap.Logger
	valkeyClient            valkey.Client
	valkeyAuditStreamExpiry time.Duration
}

// NewAuditAdapter creates an AuditAdapter whose stream retention comes from StreamRetentionFromEnv.
// If the configured retention is invalid, it logs an error and uses the 30-day default;
// use NewAuditAdapterE to get the error instead.
func NewAuditAdapter(
	logger *zap.Logger,
	valkeyClient valkey.Client,
) *AuditAdapter {
	adapter, err := NewAuditAdapterE(logger, valkeyClient)
	if err != nil {
		logger.Error("Invalid Valkey stream retention; using default",
			zap.Duration("retention", defaultRetention), zap.Error(err))
		return &AuditAdapter{
			logger:                  logger,
			valkeyClient:            valkeyClient,
			valkeyAuditStreamExpiry: defaultRetention,
		}
	}
	return adapter
}

// NewAuditAdapterE creates an AuditAdapter whose stream retention comes from StreamRetentionFromEnv.
// It returns an error wrapping ErrInvalidRetention if the configured retention is invalid.
func NewAuditAdapterE(
	logger *zap.Logger,
	valkeyClient valkey.Client,
) (*AuditAdapter, error) {
	retention, err := StreamRetentionFromEnv(logger)
	if err != nil {
		return nil, err
	}
	return &AuditAdapter{
		logger:                  logger,
		valkeyClient:            valkeyClient,
		valkeyAuditStreamExpiry: retention,
	}, nil
}

func (c *AuditAdapter) HandleEventsGet(ctx context.Context) ([]map[string]any, error) {
	result := c.valkeyClient.Do(ctx, c.valkeyClient.B().Xrevrange().Key(MdaiHubEventHistoryStreamName).End("+").Start("-").Build())
	if err := result.Error(); err != nil {
		return nil, err
	}

	resultList, err := result.ToArray()
	if err != nil {

		return nil, err
	}

	entries := make([]map[string]any, 0)
	for _, entry := range resultList {
		entryMap, err := entry.AsXRangeEntry()
		if err != nil {
			c.logger.Error("failed to convert entry to map", zap.Error(err))
			continue
		}

		if processedEntry := processEntry(entryMap); processedEntry != nil {
			entries = append(entries, processedEntry)
		}
	}
	return entries, nil
}

func processEntry(entryMap valkey.XRangeEntry) map[string]any {
	timestamp := entryMap.FieldValues["timestamp"]
	hubName := entryMap.FieldValues["hub_name"]
	eventType := entryMap.FieldValues["type"]

	switch eventType {
	case CollectorRestart:
		storedVars := showHubCollectorRestartVariables(entryMap.FieldValues)
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     "mdai/" + CollectorRestart,
			"trigger":   "mdai/" + ValkeyUpdate,
			"context": map[string]any{
				"storedVariables": storedVars,
			},
		}
	case VariableUpdated:
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     "action/" + VariableUpdated,
			"trigger":   entryMap.FieldValues["event"] + "/" + entryMap.FieldValues["status"],
			"context": map[string]any{
				"variableRef": entryMap.FieldValues["variable_ref"],
				"operation":   entryMap.FieldValues["operation"],
				"target":      entryMap.FieldValues["target"],
			},
			"payload": map[string]any{
				"variable": entryMap.FieldValues["variable"],
			},
		}
	case EventTriggered:
		return map[string]any{
			"timestamp": timestamp,
			"hubName":   hubName,
			"event":     Evaluation + "/prometheus_alert",
			"trigger":   Evaluation,
			"context": map[string]any{
				"name":       entryMap.FieldValues["name"],
				"expression": entryMap.FieldValues["expression"],
				"metric":     entryMap.FieldValues["metric_name"],
			},
			"payload": map[string]any{
				"status":              entryMap.FieldValues["status"],
				"relevantLabelValues": entryMap.FieldValues["relevant_label_values"],
				"value":               entryMap.FieldValues["value"],
			},
		}
	default:
		transformedEntry := make(map[string]any)
		for k, v := range entryMap.FieldValues {
			transformedEntry[k] = v
		}
		return transformedEntry
	}
}

func showHubCollectorRestartVariables(fields map[string]string) string {
	var storedVars []string
	ignoreKeys := map[string]bool{
		"timestamp": true,
		"type":      true,
	}
	for key, value := range fields {
		if strings.HasSuffix(key, "_CSV") && !ignoreKeys[key] && value != "" && value != "n/a" {
			storedVars = append(storedVars, value)
		}
	}
	return strings.Join(storedVars, ",")
}

func (c *AuditAdapter) insertAuditLogEvent(ctx context.Context, mdaiHubEventIter iter.Seq2[string, string]) error {
	result := c.valkeyClient.Do(ctx, c.valkeyClient.B().Xadd().Key(MdaiHubEventHistoryStreamName).Minid().Threshold(GetAuditLogTTLMinId(c.valkeyAuditStreamExpiry)).Id("*").FieldValue().FieldValueIter(mdaiHubEventIter).Build())
	if err := result.Error(); err != nil {
		c.logger.Error("failed to append event to history stream", zap.Error(err), zap.String("stream", MdaiHubEventHistoryStreamName))
		return err
	}
	return nil
}

func (c *AuditAdapter) InsertAuditLogEventFromMap(ctx context.Context, mdaiHubEventMap map[string]string) error {
	return c.insertAuditLogEvent(ctx, composeValkeyStreamIterFromMap(mdaiHubEventMap))
}

func (c *AuditAdapter) InsertAuditLogEventFromEvent(ctx context.Context, mdaiHubEvent MdaiHubEvent) error {
	return c.insertAuditLogEvent(ctx, mdaiHubEvent.ToSequence())
}

func composeValkeyStreamIterFromMap(mapToIter map[string]string) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for k, v := range mapToIter {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (c *AuditAdapter) CreateHubEvent(relevantLabels []string, alert template.Alert) MdaiHubEvent {
	metricMatch := metricRegex.FindStringSubmatch(alert.Annotations[Expression])
	metricName := ""
	if len(metricMatch) > 1 {
		metricName = metricMatch[1]
	}

	relevantLabelValues := make([]string, len(relevantLabels))
	for idx, relevantLabel := range relevantLabels {
		relevantLabelValues[idx] = alert.Labels[relevantLabel]
	}

	mdaiHubEvent := MdaiHubEvent{
		HubName:             alert.Annotations[HubName],
		Name:                alert.Annotations[AlertName],
		RelevantLabelValues: strings.Join(relevantLabelValues, ","),
		Type:                EventTriggered,
		MetricName:          metricName,
		Expression:          alert.Annotations[Expression],
		Value:               alert.Annotations[CurrentValue],
		Status:              alert.Status,
	}
	return mdaiHubEvent
}

func (c *AuditAdapter) CreateRestartEvent(mdaiCRName string, envMap map[string]string) map[string]string {
	mdaiHubEvent := map[string]string{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"hub_name":  mdaiCRName,
		"type":      CollectorRestart,
	}
	for key, value := range envMap {
		mdaiHubEvent[key] = value
	}
	return mdaiHubEvent
}

func GetAuditLogTTLMinId(valkeyAuditStreamExpiry time.Duration) string {
	return strconv.FormatInt(time.Now().Add(-valkeyAuditStreamExpiry).UnixMilli(), 10)
}

// StreamRetentionFromEnv returns the audit stream retention from VALKEY_AUDIT_STREAM_RETENTION
// (e.g. "30d", "72h"), falling back to the deprecated VALKEY_AUDIT_STREAM_EXPIRY_MS and then to
// a 30-day default. It returns an error wrapping ErrInvalidRetention if the variable in use is invalid.
// A nil logger disables logging of the chosen retention.
func StreamRetentionFromEnv(logger *zap.Logger) (time.Duration, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	if s := os.Getenv(envRetention); s != "" {
		d, err := parseHumanDuration(s)
		if err != nil {
			return 0, fmt.Errorf("%w: %s=%q: %w", ErrInvalidRetention, envRetention, s, err)
		}
		logger.Info("Using custom Valkey stream retention", zap.String("env", envRetention), zap.Duration("retention", d))
		return d, nil
	}

	if s := os.Getenv(envRetentionMsOld); s != "" {
		ms, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s=%q: %w", ErrInvalidRetention, envRetentionMsOld, s, err)
		}
		if ms < 0 {
			return 0, fmt.Errorf("%w: %s=%q: negative duration not allowed", ErrInvalidRetention, envRetentionMsOld, s)
		}
		d := time.Duration(ms) * time.Millisecond
		logger.Warn("VALKEY_AUDIT_STREAM_EXPIRY_MS is deprecated; use VALKEY_AUDIT_STREAM_RETENTION",
			zap.Duration("retention", d))
		return d, nil
	}

	logger.Info("Using default Valkey stream retention", zap.Duration("retention", defaultRetention))
	return defaultRetention, nil
}

// parseHumanDuration accepts native Go durations ("72h", "90m", "100ms")
// and also "d" (days) like "1d" or "2.5d".
func parseHumanDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	lower := strings.ToLower(s)
	if strings.HasSuffix(lower, "d") {
		num := strings.TrimSpace(lower[:len(lower)-1])
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid days %q: %w", s, err)
		}
		if f < 0 {
			return 0, fmt.Errorf("negative duration not allowed: %s", s)
		}
		hours := strconv.FormatFloat(f*24.0, 'f', -1, 64) + "h"
		return time.ParseDuration(hours)
	}

	d, err := time.ParseDuration(lower)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("negative duration not allowed: %s", s)
	}
	return d, nil
}
