package eventing

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyDefaults(t *testing.T) {
	event := &MdaiEvent{}
	event.ApplyDefaults()

	assert.NotEmpty(t, event.ID, "expected ID to be set")
	assert.False(t, event.Timestamp.IsZero(), "expected Timestamp to be set")
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   MdaiEvent
		wantErr bool
	}{
		{"missing name", MdaiEvent{HubName: "hub", Payload: "data"}, true},
		{"missing hub", MdaiEvent{Name: "test", Payload: "data"}, true},
		{"missing payload", MdaiEvent{Name: "test", HubName: "hub"}, true},
		{"valid event", MdaiEvent{Name: "test", HubName: "hub", Payload: "data"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if tt.wantErr {
				assert.Error(t, err, "expected validation error")
			} else {
				require.NoError(t, err, "expected no validation error")
			}
		})
	}
}

func TestManualVariablesActionPayload_JSONMarshaling(t *testing.T) {
	payload := VariablesActionPayload{
		VariableRef: "var1",
		DataType:    "string",
		Operation:   "set",
		Data:        "value",
	}

	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	var decoded VariablesActionPayload
	err = json.Unmarshal(jsonBytes, &decoded)
	require.NoError(t, err)
	assert.Equal(t, payload, decoded)
}

func TestVariablesActionPayload_JSON(t *testing.T) {
	tests := []struct {
		name    string
		payload VariablesActionPayload
		want    string
	}{
		{
			name:    "field omitted when empty",
			payload: VariablesActionPayload{VariableRef: "v", DataType: "set", Operation: "added", Data: "blue"},
			want:    `{"variableRef":"v","dataType":"set","operation":"added","data":"blue"}`,
		},
		{
			name:    "map entry includes field",
			payload: VariablesActionPayload{VariableRef: "v", DataType: "map", Operation: "set", Field: "color", Data: "blue"},
			want:    `{"variableRef":"v","dataType":"map","operation":"set","field":"color","data":"blue"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.payload)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
			assert.Equal(t, tt.want, string(got), "field order is part of the wire format")

			var back VariablesActionPayload
			require.NoError(t, json.Unmarshal(got, &back))
			assert.Equal(t, tt.payload, back)
		})
	}
}
