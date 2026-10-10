package eventing

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap/zapcore"
)

type MdaiEventType string
type MdaiEventConsumerGroup string

type MdaiEventSubject struct {
	// The specific MDAI Event stream this event belongs. Joined to Path with "."
	Type MdaiEventType
	// The path after the stream. Joined to preceding Type with "."
	Path string
}

func (subject MdaiEventSubject) String() string {
	return fmt.Sprintf("%s.%s", subject.Type, subject.Path)
}

func (subject MdaiEventSubject) PrefixedString(prefix string) string {
	return fmt.Sprintf("%s.%s", prefix, subject.String())
}

func NewMdaiEventSubject(stream MdaiEventType, path string) MdaiEventSubject {
	return MdaiEventSubject{
		Type: stream,
		Path: path,
	}
}

func (eventType MdaiEventType) String() string {
	return string(eventType)
}

func (consumerGroup MdaiEventConsumerGroup) String() string {
	return string(consumerGroup)
}

const (
	AlertEventType   MdaiEventType = "alert"
	VarEventType     MdaiEventType = "var"
	TriggerEventType MdaiEventType = "trigger.vars"

	AlertConsumerGroupName   MdaiEventConsumerGroup = "alert-consumer-group"
	VarsConsumerGroupName    MdaiEventConsumerGroup = "vars-consumer-group"
	TriggerConsumerGroupName MdaiEventConsumerGroup = "trigger-consumers"

	ManualVariablesEventSource  = "manual_variables_api"
	PrometheusAlertsEventSource = "prometheus"
)

// HandlerInvoker is a function type that processes MdaiEvents.
type HandlerInvoker func(event MdaiEvent) error

// MdaiEvent represents an event in the system.
type MdaiEvent struct {
	ID             string    `json:"id,omitempty"`
	Name           string    `json:"name"`    // e.g. "alert_firing"
	Version        int       `json:"version"` // schema version
	Timestamp      time.Time `json:"timestamp,omitempty"`
	Payload        string    `json:"payload"`
	Source         string    `json:"source"`    // used in subject, could not be empty
	SourceID       string    `json:"source_id"` // ex alert fingerprint
	CorrelationID  string    `json:"correlation_id,omitempty"`
	HubName        string    `json:"hub_name"`
	RecursionDepth int       `json:"recursion_depth,omitempty"` // recursion depth limit for events
}

func NewMdaiEvent(hubName string, varName string, varType string, action string, payload any) (*MdaiEvent, error) {
	payloadObj := VariablesActionPayload{
		VariableRef: varName,
		DataType:    varType,
		Operation:   action,
		Data:        payload,
	}

	payloadBytes, err := json.Marshal(payloadObj)
	if err != nil {
		return nil, err
	}

	mdaiEvent := &MdaiEvent{
		Name:    "var" + "." + action,
		HubName: hubName,
		Source:  ManualVariablesEventSource,
		Payload: string(payloadBytes),
	}
	mdaiEvent.ApplyDefaults()

	return mdaiEvent, nil
}

// MarshalLogObject signature requires it to return an error, but there's no way the code will generate one.
//
//nolint:unparam
func (mdaiEvent *MdaiEvent) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("type", mdaiEvent.Name)
	enc.AddString("id", mdaiEvent.ID)
	enc.AddString("source", mdaiEvent.Source)
	enc.AddString("source_id", mdaiEvent.SourceID)
	enc.AddString("hub_name", mdaiEvent.HubName)
	enc.AddString("payload", mdaiEvent.Payload)
	enc.AddTime("timestamp", mdaiEvent.Timestamp)
	enc.AddString("correlation_id", mdaiEvent.CorrelationID)
	enc.AddInt("recursion_depth", mdaiEvent.RecursionDepth)
	return nil
}

func (mdaiEvent *MdaiEvent) ApplyDefaults() {
	if mdaiEvent.ID == "" {
		mdaiEvent.ID = uuid.Must(uuid.NewV7()).String() // time-ordered
	}
	if mdaiEvent.Timestamp.IsZero() {
		mdaiEvent.Timestamp = time.Now().UTC()
	}

	if mdaiEvent.Version == 0 {
		mdaiEvent.Version = 1
	}
}

var errMissingRequiredFields = errors.New("missing required field")

func (mdaiEvent *MdaiEvent) Validate() error {
	if mdaiEvent.Name == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "name")
	}

	if mdaiEvent.HubName == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "hubName")
	}

	if mdaiEvent.Payload == "" {
		return fmt.Errorf("%w: %s", errMissingRequiredFields, "payload")
	}
	return nil
}

// VariablesActionPayload represents a payload for static variables actions.
type VariablesActionPayload struct {
	VariableRef string `json:"variableRef"`
	DataType    string `json:"dataType"`
	Operation   string `json:"operation"`
	// Field is the map field for map entry updates; omitted for other data types.
	Field string `json:"field,omitempty"`
	Data  any    `json:"data"`
}
