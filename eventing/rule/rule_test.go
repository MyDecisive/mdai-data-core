package rule

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mydecisive/mdai-data-core/eventing/triggers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wireForTest struct {
	Name    string `json:"name"`
	Trigger struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	} `json:"trigger"`
	Commands []Command `json:"commands"`
}

func TestRule_UnmarshalJSON_AlertOK(t *testing.T) {
	js := []byte(`{
		"name":"r1",
		"trigger":{"kind":"alert","spec":{"name":"db_down","status":"resolved"}},
		"commands":[{"type":"variable.set.add","inputs":{"k":"v"}}]
	}`)
	var r Rule
	assert.NoError(t, json.Unmarshal(js, &r))

	al, ok := r.Trigger.(*triggers.AlertTrigger)
	assert.True(t, ok)
	assert.Equal(t, "db_down", al.Name)
	assert.Equal(t, "resolved", al.Status)
	assert.Equal(t, "r1", r.Name)
	assert.Len(t, r.Commands, 1)
	assert.Equal(t, CmdVarSetAdd, r.Commands[0].Type)
}

func TestRule_UnmarshalJSON_VariableOK(t *testing.T) {
	js := []byte(`{
		"name":"r2",
		"trigger":{"kind":"variable","spec":{"name":"cpu","update_type":"added"}},
		"commands":[]
	}`)
	var r Rule
	assert.NoError(t, json.Unmarshal(js, &r))

	vt, ok := r.Trigger.(*triggers.VariableTrigger)
	assert.True(t, ok)
	assert.Equal(t, "cpu", vt.Name)
	assert.Equal(t, "added", vt.UpdateType)
}

func TestRule_UnmarshalJSON_UnknownTriggerKind(t *testing.T) {
	js := []byte(`{
		"name":"r3",
		"trigger":{"kind":"wat","spec":{}},
		"commands":[]
	}`)
	var r Rule
	err := json.Unmarshal(js, &r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "trigger: unknown trigger kind")
}

func TestRule_UnmarshalJSON_AlertSpecUnknownField(t *testing.T) {
	js := []byte(`{
		"name":"r4",
		"trigger":{"kind":"alert","spec":{"name":"x","bogus":1}},
		"commands":[]
	}`)
	var r Rule
	err := json.Unmarshal(js, &r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "trigger: alert spec")
}

func TestRule_UnmarshalJSON_UnknownTopLevelField(t *testing.T) {
	js := []byte(`{
		"name":"r5",
		"unexpected": true,
		"trigger":{"kind":"alert","spec":{"name":"x"}},
		"commands":[]
	}`)
	var r Rule
	err := json.Unmarshal(js, &r)
	assert.Error(t, err)
	// json.Decoder with DisallowUnknownFields -> "unknown field"
	assert.Contains(t, err.Error(), "unknown field")
}

func TestRule_UnmarshalJSON_InvalidCommandType(t *testing.T) {
	tests := []struct {
		name    string
		cmds    string
		wantErr string
	}{
		{"unknown type", `[{"type":"variable.set","inputs":{}}]`, `commands[0]: invalid command type "variable.set"`},
		{"second command invalid", `[{"type":"webhook.call","inputs":{}},{"type":"Webhook.Call","inputs":{}}]`, `commands[1]: invalid command type "Webhook.Call"`},
		{"missing type", `[{"inputs":{}}]`, `commands[0]: invalid command type ""`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := []byte(`{"name":"r","trigger":{"kind":"alert","spec":{"name":"x"}},"commands":` + tc.cmds + `}`)
			var r Rule
			err := json.Unmarshal(js, &r)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestRule_UnmarshalJSON_AllCommandTypesAccepted(t *testing.T) {
	cmds := make([]Command, 0, len(AllCommandTypes))
	for _, ct := range AllCommandTypes {
		cmds = append(cmds, Command{Type: ct, Inputs: json.RawMessage(`{}`)})
	}
	in := Rule{Name: "r", Trigger: &triggers.VariableTrigger{Name: "cpu", UpdateType: "set"}, Commands: cmds}

	out, err := json.Marshal(in)
	require.NoError(t, err)

	var got Rule
	require.NoError(t, json.Unmarshal(out, &got))
	assert.Equal(t, in, got)
}

func TestRule_UnmarshalJSON_StrictTriggerEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		trigger string
		wantErr string
	}{
		{"unknown envelope field", `{"kind":"alert","spec":{"name":"x"},"extra":1}`, `trigger: trigger envelope: json: unknown field "extra"`},
		{"missing spec", `{"kind":"alert"}`, `trigger: trigger envelope: missing spec`},
		{"missing kind", `{"spec":{"name":"x"}}`, `trigger: trigger envelope: missing kind`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := []byte(`{"name":"r","trigger":` + tc.trigger + `,"commands":[]}`)
			var r Rule
			err := json.Unmarshal(js, &r)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestRule_UnmarshalJSON_MissingTrigger(t *testing.T) {
	var r Rule
	err := json.Unmarshal([]byte(`{"name":"r","commands":[]}`), &r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trigger: trigger envelope: missing trigger")
}

func TestRule_MarshalJSON_AlertOK(t *testing.T) {
	in := map[string]string{"k": "v"}
	inRaw, _ := json.Marshal(in)
	r := Rule{
		Name:    "r1",
		Trigger: &triggers.AlertTrigger{Name: "db_down", Status: "firing"},
		Commands: []Command{
			{Type: CmdVarSetAdd, Inputs: json.RawMessage(inRaw)},
		},
	}

	out, err := json.Marshal(r)
	assert.NoError(t, err)

	var wire = wireForTest{}
	assert.NoError(t, json.Unmarshal(out, &wire))
	assert.Equal(t, "r1", wire.Name)
	assert.Equal(t, "alert", wire.Trigger.Kind)

	var spec triggers.AlertTrigger
	assert.NoError(t, json.Unmarshal(wire.Trigger.Spec, &spec))
	assert.Equal(t, "db_down", spec.Name)
	assert.Equal(t, "firing", spec.Status)

	assert.Len(t, wire.Commands, 1)
	assert.Equal(t, CmdVarSetAdd, wire.Commands[0].Type)

	var got map[string]string
	assert.NoError(t, json.Unmarshal(wire.Commands[0].Inputs, &got))
	assert.Equal(t, "v", got["k"])
}

func TestRule_MarshalJSON_VariableOK(t *testing.T) {
	r := Rule{
		Name:    "r2",
		Trigger: &triggers.VariableTrigger{Name: "cpu", UpdateType: "set"},
	}
	out, err := json.Marshal(r)
	assert.NoError(t, err)

	var wire = wireForTest{}
	assert.NoError(t, json.Unmarshal(out, &wire))
	assert.Equal(t, "variable", wire.Trigger.Kind)

	var spec triggers.VariableTrigger
	assert.NoError(t, json.Unmarshal(wire.Trigger.Spec, &spec))
	assert.Equal(t, "cpu", spec.Name)
	assert.Equal(t, "set", spec.UpdateType)
}

func TestRule_MarshalJSON_MissingTrigger(t *testing.T) {
	r := Rule{Name: "r-missing"}
	_, err := json.Marshal(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing trigger")
}

func TestRule_MarshalJSON_NilTypedAlertTrigger(t *testing.T) {
	var at *triggers.AlertTrigger // typed nil
	r := Rule{Name: "r-nil-alert", Trigger: at}
	_, err := json.Marshal(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil alert trigger")
}

func TestRule_MarshalJSON_NilTypedVariableTrigger(t *testing.T) {
	var vt *triggers.VariableTrigger // typed nil
	r := Rule{Name: "r-nil-var", Trigger: vt}
	_, err := json.Marshal(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil variable trigger")
}

// A trigger that implements the interface but isn't a known concrete type.
type fakeTrigger struct{}

func (*fakeTrigger) Kind() string                { return "bogus" }
func (*fakeTrigger) Match(triggers.Context) bool { return false }

func TestRule_MarshalJSON_UnsupportedTriggerKind(t *testing.T) {
	r := Rule{Name: "r-unsupported", Trigger: &fakeTrigger{}}
	_, err := json.Marshal(r)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported trigger kind")
}

func TestParseCommandType_Valid_All(t *testing.T) {
	t.Parallel()

	assert.True(t, len(AllCommandTypes) > 0, "no command types defined")

	for _, want := range AllCommandTypes {
		t.Run("valid_"+string(want), func(t *testing.T) {
			t.Parallel()
			got, err := ParseCommandType(string(want))
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestParseCommandType_InvalidCases(t *testing.T) {
	t.Parallel()

	assert.True(t, len(AllCommandTypes) > 0, "no command types defined")
	var aValid = string(AllCommandTypes[0])

	cases := []struct {
		name string
		in   string
	}{
		{"Unknown", "not.a.real.command"},
		{"Empty", ""},
		{"Whitespace", " " + aValid + " "},       // current impl doesn't trim -> invalid
		{"CaseChanged", strings.ToUpper(aValid)}, // current impl is case-sensitive
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ct, err := ParseCommandType(tc.in)
			assert.Error(t, err)
			assert.Equal(t, CommandType(""), ct)

			msg := err.Error()
			assert.Contains(t, msg, `invalid command type`)
			// Ensure the error lists the allowed values (order-insensitive).
			for _, allowed := range AllCommandTypes {
				assert.Containsf(t, msg, string(allowed), "allowed list should mention %q", allowed)
			}
		})
	}
}
