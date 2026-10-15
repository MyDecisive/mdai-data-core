package triggers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTrigger_AlertOK(t *testing.T) {
	raw := json.RawMessage(`{"kind":"alert","spec":{"name":"db_down","status":"firing"}}`)
	got, err := BuildTrigger(raw)
	assert.NoError(t, err)

	al, ok := got.(*AlertTrigger)
	assert.True(t, ok)
	assert.Equal(t, "db_down", al.Name)
	assert.Equal(t, "firing", al.Status)
}

func TestBuildTrigger_VariableOK(t *testing.T) {
	raw := json.RawMessage(`{"kind":"variable","spec":{"name":"cpu","update_type":"set"}}`)
	got, err := BuildTrigger(raw)
	assert.NoError(t, err)

	vt, ok := got.(*VariableTrigger)
	assert.True(t, ok)
	assert.Equal(t, "cpu", vt.Name)
	assert.Equal(t, "set", vt.UpdateType)
}

func TestBuildTrigger_UnknownKind(t *testing.T) {
	raw := json.RawMessage(`{"kind":"weird","spec":{}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "unknown trigger kind")
}

func TestBuildTrigger_AlertSpecUnknownField(t *testing.T) {
	raw := json.RawMessage(`{"kind":"alert","spec":{"name":"x","bogus":1}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "alert spec")
}

func TestBuildTrigger_VariableSpecUnknownField(t *testing.T) {
	raw := json.RawMessage(`{"kind":"variable","spec":{"name":"x","update_type":"set","bogus":1}}`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "variable spec")
}

func TestBuildTrigger_BadEnvelope(t *testing.T) {
	// not an object → envelope unmarshal error
	raw := json.RawMessage(`[]`)
	got, err := BuildTrigger(raw)
	assert.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "trigger envelope")
}

func TestBuildTrigger_StrictEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{"unknown envelope field", `{"kind":"alert","spec":{"name":"x"},"bogus":1}`, `unknown field "bogus"`},
		{"misspelled kind", `{"kinds":"alert","spec":{"name":"x"}}`, `unknown field "kinds"`},
		{"misspelled spec", `{"kind":"alert","specs":{"name":"x"}}`, `unknown field "specs"`},
		{"missing kind", `{"spec":{"name":"x"}}`, "trigger envelope: missing kind"},
		{"empty kind", `{"kind":"","spec":{"name":"x"}}`, "trigger envelope: missing kind"},
		{"missing spec", `{"kind":"alert"}`, `trigger envelope: missing spec for kind "alert"`},
		{"null spec", `{"kind":"variable","spec":null}`, `trigger envelope: missing spec for kind "variable"`},
		{"empty input", ``, "trigger envelope: missing trigger"},
		{"null input", `null`, "trigger envelope: missing kind"},
		{"trailing data after envelope", `{"kind":"alert","spec":{"name":"x"}} {}`, "trigger envelope: unexpected data after JSON value"},
		{"spec not an object", `{"kind":"alert","spec":"x"}`, "alert spec"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildTrigger(json.RawMessage(tc.raw))
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestBuildTrigger_EmptySpecObjectAccepted(t *testing.T) {
	// An empty spec object is well-formed; Match rejects the empty name at runtime.
	got, err := BuildTrigger(json.RawMessage(`{"kind":"alert","spec":{}}`))
	require.NoError(t, err)
	assert.Equal(t, &AlertTrigger{}, got)
}
