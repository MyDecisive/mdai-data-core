package interpolation

import (
	"testing"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestEngine_Interpolate(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	event := &eventing.MdaiEvent{
		ID:             "test-id",
		Name:           "test-event",
		Timestamp:      timestamp,
		Payload:        `{"user":{"name":"John","age":30},"level":"info","complex":{"nested":{"value":"deep"}}}`,
		Source:         "test-source",
		SourceID:       "source-123",
		CorrelationID:  "corr-456",
		HubName:        "test-hub",
		RecursionDepth: 456,
	}

	tests := []struct {
		name     string
		input    string
		expected string
		event    *eventing.MdaiEvent
	}{
		{
			name:     "simple event field",
			input:    "ID: ${trigger:id}",
			expected: "ID: test-id",
			event:    event,
		},
		{
			name:     "event name field",
			input:    "Event: ${trigger:name}",
			expected: "Event: test-event",
			event:    event,
		},
		{
			name:     "event name field with default (not used)",
			input:    "Event: ${trigger:name:-default-event}",
			expected: "Event: test-event",
			event:    event,
		},
		{
			name:     "event name field (empty)",
			input:    "Event: ${trigger:name}",
			expected: "Event: ${trigger:name}",
			event:    &eventing.MdaiEvent{ID: "test-id", Name: ""},
		},
		{
			name:     "event name field (empty) with default",
			input:    "Event: ${trigger:name:-default-event}",
			expected: "Event: default-event",
			event:    &eventing.MdaiEvent{ID: "test-id", Name: ""},
		},
		{
			name:     "event name field (nil event)",
			input:    "Event: ${trigger:name:-no-event}",
			expected: "Event: no-event",
			event:    nil,
		},
		{
			name:     "timestamp field",
			input:    "Time: ${trigger:timestamp}",
			expected: "Time: 2023-01-01T12:00:00Z",
			event:    event,
		},
		{
			name:     "correlation_id field",
			input:    "Correlation_id: ${trigger:correlation_id}",
			expected: "Correlation_id: corr-456",
			event:    event,
		},
		{
			name:     "recursion_depth field",
			input:    "recursion_depth: ${trigger:recursion_depth}",
			expected: "recursion_depth: 456",
			event:    event,
		},
		{
			name:     "hub_name field",
			input:    "hub_name: ${trigger:hub_name}",
			expected: "hub_name: test-hub",
			event:    event,
		},
		{
			name:     "source_id field",
			input:    "Source: ${trigger:source_id}",
			expected: "Source: source-123",
			event:    event,
		},
		{
			name:     "payload field with explicit prefix",
			input:    "Level: ${trigger:payload.level}",
			expected: "Level: info",
			event:    event,
		},
		{
			name:     "nested payload field with explicit prefix",
			input:    "User: ${trigger:payload.user.name}",
			expected: "User: John",
			event:    event,
		},
		{
			name:     "deeply nested payload field with explicit prefix",
			input:    "Deep: ${trigger:payload.complex.nested.value}",
			expected: "Deep: deep",
			event:    event,
		},
		{
			name:     "with default value - payload field exists",
			input:    "User: ${trigger:payload.user.name:-Anonymous}",
			expected: "User: John",
			event:    event,
		},
		{
			name:     "with default value - payload field missing",
			input:    "Status: ${trigger:payload.status:-unknown}",
			expected: "Status: unknown",
			event:    event,
		},
		{
			name:     "invalid event field - should fail",
			input:    "Invalid: ${trigger:level:-default}",
			expected: "Invalid: default",
			event:    event,
		},
		{
			name:     "invalid nested event field - should fail",
			input:    "Invalid: ${trigger:user.name:-default}",
			expected: "Invalid: default",
			event:    event,
		},
		{
			name:     "multiple interpolations",
			input:    "Event ${trigger:name} from ${trigger:source} at ${trigger:timestamp}",
			expected: "Event test-event from test-source at 2023-01-01T12:00:00Z",
			event:    event,
		},
		{
			name:     "missing event field no default - returns original",
			input:    "Missing: ${trigger:nonexistent}",
			expected: "Missing: ${trigger:nonexistent}",
			event:    event,
		},
		{
			name:     "missing payload field no default - returns original",
			input:    "Missing: ${trigger:payload.nonexistent}",
			expected: "Missing: ${trigger:payload.nonexistent}",
			event:    event,
		},
		{
			name:     "nil event",
			input:    "Value: ${trigger:id:-default}",
			expected: "Value: default",
			event:    nil,
		},
		{
			name:     "empty payload - payload field with default",
			input:    "Value: ${trigger:payload.user.name:-default}",
			expected: "Value: default",
			event:    &eventing.MdaiEvent{ID: "test"},
		},
		{
			name:     "invalid JSON payload - payload field with default",
			input:    "Value: ${trigger:payload.field:-default}",
			expected: "Value: default",
			event:    &eventing.MdaiEvent{Payload: "{invalid json}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, tt.event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEngine_ComplexPayloadValues(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID: "test-id",
		Payload: `{
            "string_val": "hello",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": true,
            "object_val": {"nested": "value"},
            "array_val": [1, 2, 3]
        }`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"string value", "${trigger:payload.string_val}", "hello"},
		{"integer value", "${trigger:payload.int_val}", "42"},
		{"float value", "${trigger:payload.float_val}", "3.14"},
		{"boolean value", "${trigger:payload.bool_val}", "true"},
		{"object value (JSON marshaled)", "${trigger:payload.object_val}", `{"nested":"value"}`},
		{"array value (JSON marshaled)", "${trigger:payload.array_val}", `[1,2,3]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEngine_EventFieldsWithEmptyValues(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:            "test-id",
		Name:          "",
		Source:        "test-source",
		SourceID:      "",
		CorrelationID: "",
		HubName:       "test-hub",
		Payload:       `{"level":"info"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty name field with default", "${trigger:name:-default-name}", "default-name"},
		{"empty source_id with default", "${trigger:source_id:-no-source}", "no-source"},
		{"zero timestamp with default", "${trigger:timestamp:-no-time}", "no-time"},
		{"non-empty field", "${trigger:id}", "test-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEngine_ErrorLogging(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: `{"existing":"value"}`,
	}

	tests := []struct {
		name          string
		input         string
		expectedError bool
	}{
		{"missing event field without default", "${trigger:nonexistent}", true},
		{"missing payload field without default", "${trigger:payload.nonexistent}", true},
		{"unsupported scope", "${env:PATH}", true},
		{"valid field", "${trigger:id}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			if tt.expectedError {
				assert.Equal(t, tt.input, result, "Expected original input")
			}
		})
	}
}

func TestEngine_NestedPayloadMissingPath(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: `{"user":{"name":"John"},"level":"info"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"missing nested payload path with default", "${trigger:payload.user.age:-unknown}", "unknown"},
		{"missing root payload path with default", "${trigger:payload.config.setting:-default}", "default"},
		{"path through non-object in payload with default", "${trigger:payload.level.subfield:-default}", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEngine_EventFieldsOnly(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Name:    "test-event",
		Payload: `{"name":"payload-name","id":"payload-id"}`,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"event name field (not payload)", "${trigger:name}", "test-event"},
		{"event id field (not payload)", "${trigger:id}", "test-id"},
		{"payload name field with prefix", "${trigger:payload.name}", "payload-name"},
		{"payload id field with prefix", "${trigger:payload.id}", "payload-id"},
		{"non-event field fails", "Value: ${trigger:custom_field:-default}", "Value: default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.Interpolate(tt.input, event)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEngine_PayloadRawField(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	payloadContent := `{"user":{"name":"John"},"level":"info"}`
	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: payloadContent,
	}

	result := engine.Interpolate("Raw: ${trigger:payload}", event)
	expected := "Raw: " + payloadContent
	assert.Equal(t, expected, result)
}

func TestEngine_TemplateScope_BasicAndDefaults(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	tpl := map[string]string{
		"service": "mdai_service",
		"url":     "http://localhost:9090/alerts",
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic template keys", "svc=${template:service} link=${template:url}", "svc=mdai_service link=http://localhost:9090/alerts"},
		{"missing template key uses default", "x=${template:missing:-def}", "x=def"},
		{"empty input", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.InterpolateWithValues(tt.input, nil, tpl)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestEngine_TemplateScope_EmptyStringFallsBackToDefault(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	tpl := map[string]string{
		"empty": "",
	}
	got := engine.InterpolateWithValues("${template:empty:-fallback}", nil, tpl)
	assert.Equal(t, "fallback", got)
}

func TestEngine_TriggerAndTemplate_Together(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:     "abc-123",
		Name:   "evt",
		Source: "gateway",
	}

	tpl := map[string]string{
		"service": "mdai_service",
	}

	input := "n=${trigger:name} s=${trigger:source} svc=${template:service} id=${trigger:id}"
	want := "n=evt s=gateway svc=mdai_service id=abc-123"

	got := engine.InterpolateWithValues(input, event, tpl)
	assert.Equal(t, want, got)
}

func TestEngine_TemplateDoesNotShadowTrigger(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		Name: "event-name",
	}
	tpl := map[string]string{
		"name": "template-name",
	}

	got := engine.InterpolateWithValues("${trigger:name} / ${template:name}", event, tpl)
	want := "event-name / template-name"
	assert.Equal(t, want, got)
}

func TestEngine_InterpolateMapWithSources(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:     "id-1",
		Source: "hub",
	}
	tpl := map[string]string{
		"channel": "alerts",
	}

	in := map[string]string{
		"a": "cid=${trigger:id}",
		"b": "ch=${template:channel}",
		"c": "src=${trigger:source} miss=${template:oops:-none}",
	}
	out := engine.InterpolateMapWithSources(in, &TriggerSource{Event: event}, TemplateSource{Values: tpl})

	expected := map[string]string{
		"a": "cid=id-1",
		"b": "ch=alerts",
		"c": "src=hub miss=none",
	}
	assert.Equal(t, expected, out)
}

func TestEngine_InterpolateWithValues_NilTemplateMap(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	input := "x=${template:key:-fallback}"
	got := engine.InterpolateWithValues(input, nil, nil)
	assert.Equal(t, "x=fallback", got, "InterpolateWithValues() with nil map should use default")
}

func TestEngine_InterpolateWithSources(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	source1 := TemplateSource{Values: map[string]string{"key": "value1"}}
	source2 := TemplateSource{Values: map[string]string{"key": "value2"}}
	var nilSource ValueSource

	tests := []struct {
		name     string
		input    string
		sources  []ValueSource
		expected string
	}{
		{
			name:     "unknown scope",
			input:    "x=${unknown:key:-d}",
			sources:  []ValueSource{},
			expected: "x=${unknown:key:-d}",
		},
		{
			name:     "duplicate scope uses first",
			input:    "Result: ${template:key}",
			sources:  []ValueSource{source1, source2},
			expected: "Result: value1",
		},
		{
			name:     "duplicate scope uses first (reversed)",
			input:    "Result: ${template:key}",
			sources:  []ValueSource{source2, source1},
			expected: "Result: value2",
		},
		{
			name:     "empty input",
			input:    "",
			sources:  []ValueSource{},
			expected: "",
		},
		{
			name:     "mixed valid and nil sources",
			input:    "Result: ${template:key}",
			sources:  []ValueSource{nilSource, source1},
			expected: "Result: value1",
		},
		{
			name:     "mixed valid and nil sources (reversed)",
			input:    "Result: ${template:key}",
			sources:  []ValueSource{source1, nilSource},
			expected: "Result: value1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.InterpolateWithSources(tt.input, tt.sources...)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestEngine_ReplaceMatchWithSources_InvalidSyntax(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	// This input is invalid because it's missing the colon between scope and field.
	invalidMatch := "${trigger-id}"

	// The function should return the original string if the regex doesn't match.
	result := engine.replaceMatchWithSources(invalidMatch, nil)

	assert.Equal(t, invalidMatch, result, "Expected original string for invalid syntax")
}

func TestEngine_ReplaceMatchWithSources_NonStringValue(t *testing.T) {
	engine := NewEngine(zap.NewNop())

	event := &eventing.MdaiEvent{
		ID:      "test-id",
		Payload: `{"value": 12345}`,
	}

	sources := map[string]ValueSource{
		"trigger": &TriggerSource{Event: event},
	}

	// Simulate the call from ReplaceAllStringFunc
	result := engine.replaceMatchWithSources("${trigger:payload.value}", sources)

	assert.Equal(t, "12345", result, "should correctly format the integer value to a string")
}

func TestConvertToString_JSONUnmarshalTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"float64 (int-like)", float64(123), "123"}, // json numbers -> float64
		{"float64 (decimal)", 3.14, "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"nil", nil, ""},
		{"json object as map", map[string]any{"k": "v"}, `{"k":"v"}`},
		{"json array as slice", []any{1.0, "x", false}, `[1,"x",false]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertToString(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestConvertToString_NumberFormatting(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"int-like as float64", float64(1), "1"},
		{"decimal no padding", 1.5, "1.5"},
		{"large but not scientific", 123456.0, "123456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertToString(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestEngine_PayloadHyphenKey(t *testing.T) {
	e := NewEngine(zap.NewNop())
	ev := &eventing.MdaiEvent{Payload: `{"error-rate":"high"}`}

	got := e.Interpolate("rate=${trigger:payload.error-rate}", ev)
	assert.Equal(t, "rate=high", got)
}

func TestEngine_PayloadNumberLexicalForm(t *testing.T) {
	e := NewEngine(zap.NewNop())
	ev := &eventing.MdaiEvent{Payload: `{"n": 1.00}`}

	got := e.Interpolate("${trigger:payload.n}", ev)
	assert.Equal(t, "1.00", got)
}
