package interpolation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"go.uber.org/zap"
)

type ValueSource interface {
	Scope() string
	Lookup(field string) (string, bool)
}

// TriggerSource reads fields from MdaiEvent (incl. payload.* JSON path)
type TriggerSource struct {
	Event       *eventing.MdaiEvent
	payloadOnce sync.Once
	payloadMap  map[string]any
	payloadErr  error
}

func (s *TriggerSource) Scope() string { return "trigger" }

func (s *TriggerSource) Lookup(field string) (string, bool) {
	return getEventFieldValue(field, s)
}

func (s *TriggerSource) payload() (map[string]any, error) {
	s.payloadOnce.Do(func() { // unmarshal once and cache
		if s.Event == nil || s.Event.Payload == "" {
			return
		}
		s.payloadMap, s.payloadErr = unmarshalPayloadUseNumber(s.Event.Payload)
	})
	return s.payloadMap, s.payloadErr
}

// TemplateSource reads from a flat map[string]string
type TemplateSource struct {
	Values map[string]string
}

func (s TemplateSource) Scope() string { return "template" }

func (s TemplateSource) Lookup(field string) (string, bool) {
	if s.Values == nil {
		return "", false
	}
	v, ok := s.Values[field]
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// Engine handles OTel-inspired interpolation for trigger events
type Engine struct {
	pattern *regexp.Regexp
	logger  *zap.Logger
}

func NewEngine(logger *zap.Logger) *Engine {
	pattern := regexp.MustCompile(`\$\{([^:}]+):([^}:]+)(?::-([^}]*))?}`)
	return &Engine{
		pattern: pattern,
		logger:  logger,
	}
}

// Interpolate processes a string and replaces interpolation expressions with actual values
// Back-compat: only trigger scope via MdaiEvent
func (e *Engine) Interpolate(input string, event *eventing.MdaiEvent) string {
	return e.InterpolateWithSources(input, &TriggerSource{Event: event})
}

// InterpolateWithValues trigger + template scopes
func (e *Engine) InterpolateWithValues(input string, event *eventing.MdaiEvent, templateValues map[string]string) string {
	return e.InterpolateWithSources(input,
		&TriggerSource{Event: event},
		TemplateSource{Values: templateValues},
	)
}

// InterpolateWithSources provide any set of sources (each defines its Scope() name)
func (e *Engine) InterpolateWithSources(input string, sources ...ValueSource) string {
	if input == "" {
		return ""
	}
	scopeMap := make(map[string]ValueSource, len(sources))
	for _, s := range sources {
		if s == nil {
			continue
		}

		sv := reflect.ValueOf(s)
		if sv.Kind() == reflect.Ptr && sv.IsNil() {
			continue
		}

		if _, exists := scopeMap[s.Scope()]; exists {
			e.logger.Warn("duplicate scope in interpolation sources, skipping", zap.String("scope", s.Scope()))
			continue
		}
		scopeMap[s.Scope()] = s
	}

	return e.pattern.ReplaceAllStringFunc(input, func(match string) string {
		return e.replaceMatchWithSources(match, scopeMap)
	})
}

// replaceMatch processes a single interpolation match
func (e *Engine) replaceMatchWithSources(match string, sources map[string]ValueSource) string {
	matches := e.pattern.FindStringSubmatch(match)
	if len(matches) < 3 {
		e.logger.Error("failed to match regex, missing required elements",
			zap.Bool("interpolation_made", false),
			zap.String("match", match),
		)
		return match
	}

	scope := strings.TrimSpace(matches[1])
	field := strings.TrimSpace(matches[2])
	defaultValue := ""
	if len(matches) > 3 && matches[3] != "" {
		defaultValue = matches[3]
	}

	src, ok := sources[scope]
	if !ok {
		e.logger.Error("unsupported scope",
			zap.Bool("interpolation_made", false),
			zap.String("match", match),
			zap.String("scope", scope),
			zap.String("field", field),
		)
		return match
	}

	val, found := src.Lookup(field)
	if !found {
		if defaultValue != "" {
			e.logger.Warn("field not found, using default",
				zap.Bool("interpolation_made", true),
				zap.String("scope", scope),
				zap.String("field", field),
				zap.String("default", defaultValue),
			)
			return defaultValue
		}
		e.logger.Warn("field not found and no default value provided",
			zap.Bool("interpolation_made", false),
			zap.String("match", match),
			zap.String("scope", scope),
			zap.String("field", field),
		)
		return match
	}

	e.logger.Info("interpolation succeeded",
		zap.Bool("interpolation_made", true),
		zap.String("match", match),
		zap.String("scope", scope),
		zap.String("field", field),
		zap.String("value", val),
	)

	return val
}

// InterpolateMapWithSources interpolate map with arbitrary sources (e.g., trigger+template)
func (e *Engine) InterpolateMapWithSources(in map[string]string, sources ...ValueSource) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if v == "" {
			out[k] = v
			continue
		}
		out[k] = e.InterpolateWithSources(v, sources...)
	}
	return out
}

// JSON-only stringifier (types produced by json.Unmarshal into interface{}).
func convertToString(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case float64: // for legacy paths without UseNumber
		return strconv.FormatFloat(x, 'g', -1, 64)
	case json.Number: // preserves original formatting
		return x.String()
	case map[string]any, []any:
		if b, err := json.Marshal(x); err == nil {
			return string(b)
		}
	}
	return fmt.Sprint(v)
}

func getNestedValue(data map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		value, exists := current[part]
		if !exists {
			return nil, false
		}

		if i == len(parts)-1 {
			return value, true
		}
		nextMap, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = nextMap
	}

	return nil, false
}

func unmarshalPayloadUseNumber(raw string) (map[string]any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func getEventFieldValue(field string, ts *TriggerSource) (string, bool) {
	if ts == nil {
		return "", false
	}

	event := ts.Event
	if event == nil {
		return "", false
	}

	parts := strings.Split(field, ".")

	if len(parts) == 1 {
		switch field {
		case "id":
			return event.ID, event.ID != ""
		case "name":
			return event.Name, event.Name != ""
		case "timestamp":
			if !event.Timestamp.IsZero() {
				return event.Timestamp.Format(time.RFC3339), true
			}
			return "", false
		case "payload":
			return event.Payload, event.Payload != ""
		case "source":
			return event.Source, event.Source != ""
		case "source_id":
			return event.SourceID, event.SourceID != ""
		case "correlation_id":
			return event.CorrelationID, event.CorrelationID != ""
		case "hub_name":
			return event.HubName, event.HubName != ""
		case "recursion_depth":
			return strconv.Itoa(event.RecursionDepth), true
		}
	}

	if strings.HasPrefix(field, "payload.") {
		payload, err := ts.payload()
		if err != nil || payload == nil {
			return "", false
		}
		value, ok := getNestedValue(payload, strings.TrimPrefix(field, "payload."))
		if !ok {
			return "", false
		}
		return convertToString(value), true
	}

	return "", false
}
