package triggers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	KindAlert    = "alert"
	KindVariable = "variable"
)

// Trigger interface for various event triggers
type Trigger interface {
	// Match event with trigger
	Match(ctx Context) bool
	// Kind of the trigger
	Kind() string
}

// Context for event matching
// AlertCtx and VariableCtx represent different types of context for different trigger types
type Context struct {
	Alert    *AlertCtx    // nil unless this was an alert
	Variable *VariableCtx // nil unless this was a variable
	Now      time.Time
}

// BuildTrigger parses raw JSON of the form {"kind": ..., "spec": {...}} into a Trigger.
// Decoding is strict: unknown fields in the envelope or the spec, trailing data, and a missing
// kind or spec are all rejected so configuration mistakes fail early.
// Returns an error if the raw JSON is invalid or the trigger kind is unknown.
func BuildTrigger(raw json.RawMessage) (Trigger, error) {
	var envelope struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("trigger envelope: missing trigger")
	}
	if err := decodeStrict(raw, &envelope); err != nil {
		return nil, fmt.Errorf("trigger envelope: %w", err)
	}
	if envelope.Kind == "" {
		return nil, errors.New("trigger envelope: missing kind")
	}
	if len(envelope.Spec) == 0 || bytes.Equal(envelope.Spec, []byte("null")) {
		return nil, fmt.Errorf("trigger envelope: missing spec for kind %q", envelope.Kind)
	}
	switch envelope.Kind {
	case KindAlert:
		var alert AlertTrigger
		if err := decodeStrict(envelope.Spec, &alert); err != nil {
			return nil, fmt.Errorf("alert spec: %w", err)
		}
		return &alert, nil
	case KindVariable:
		var variable VariableTrigger
		if err := decodeStrict(envelope.Spec, &variable); err != nil {
			return nil, fmt.Errorf("variable spec: %w", err)
		}
		return &variable, nil
	default:
		return nil, fmt.Errorf("unknown trigger kind %q", envelope.Kind)
	}
}

// decodeStrict decodes a single JSON value into v, rejecting unknown fields and trailing data.
func decodeStrict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON value")
	}
	return nil
}
