package triggers

import (
	"go.uber.org/zap/zapcore"
)

// VariableCtx represents contextual information for variable-related events. It's used in matching rules.
type VariableCtx struct {
	Name       string `json:"name"`            // variable key
	UpdateType string `json:"update_type"`     // "added" | "removed" | "set", the operation from the variable-update event
	Value      string `json:"value,omitempty"` // current value if applicable
}

// VariableTrigger matches events based on variable name and update type.
type VariableTrigger struct {
	// Name is required by Match and compared exactly; empty names do not match.
	Name string `json:"name,omitempty"`
	// UpdateType is optional ("added", "removed" or "set"); when empty, any update type matches.
	UpdateType string `json:"update_type,omitempty"`
}

// Match checks if the trigger matches the given context.
func (t *VariableTrigger) Match(ctx Context) bool {
	// Variable Name is required; reject empty triggers
	if t.Name == "" {
		return false
	}
	v := ctx.Variable
	if v == nil {
		return false
	}
	if t.Name != "" && v.Name != t.Name {
		return false
	}
	if t.UpdateType != "" && v.UpdateType != t.UpdateType {
		return false
	}
	return true
}

// MarshalLogObject encodes the trigger as a log object.
func (t *VariableTrigger) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("kind", "variable")
	if t.Name != "" {
		enc.AddString("name", t.Name)
	}
	if t.UpdateType != "" {
		enc.AddString("update_type", t.UpdateType)
	}
	return nil
}

// Kind returns the kind of trigger.
func (t *VariableTrigger) Kind() string { return KindVariable }
