package triggers

import (
	"go.uber.org/zap/zapcore"
)

// AlertCtx represents contextual information for alert-related events. It's used in matching rules.
type AlertCtx struct {
	Name   string
	Status string // "firing" | "resolved"
}

// AlertTrigger matches events based on alert name and status.
type AlertTrigger struct {
	// Name is required by Match; empty names do not match.
	Name string `json:"name,omitempty"`
	// Status is optional; when empty, status is ignored.
	Status string `json:"status,omitempty"`
}

// Match checks if the trigger matches the given context.
func (t *AlertTrigger) Match(ctx Context) bool {
	// Alert Name is required; reject empty triggers
	if t.Name == "" {
		return false
	}
	a := ctx.Alert
	if a == nil {
		return false
	}
	if t.Name != "" && a.Name != t.Name {
		return false
	}
	if t.Status != "" && a.Status != t.Status {
		return false
	}
	return true
}

// MarshalLogObject encodes the trigger as a log object.
func (t *AlertTrigger) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("kind", "alert")
	if t.Name != "" {
		enc.AddString("name", t.Name)
	}
	if t.Status != "" {
		enc.AddString("status", t.Status)
	}
	return nil
}

// Kind returns the kind of trigger.
func (t *AlertTrigger) Kind() string { return KindAlert }
