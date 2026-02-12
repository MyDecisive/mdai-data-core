package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/internal/mocks/eventing/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	vmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newAdapterWithMocks(t *testing.T) (*HandlerAdapter, *vmock.Client, *publisher.MockPublisher, *gomock.Controller) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClient := vmock.NewClient(ctrl)
	mockPub := publisher.NewMockPublisher(ctrl)
	logger := zap.NewNop()

	adapter := NewHandlerAdapter(mockClient, logger, mockPub)
	return adapter, mockClient, mockPub, ctrl
}

func TestSetStringValue(t *testing.T) {
	hubName := "my-hub"
	variableKey := "my-var"
	value := "my-value"
	correlationId := "corr-id-123"
	ctx := context.Background()

	testCases := []struct {
		name           string
		setupMocks     func(client *vmock.Client, pub *publisher.MockPublisher)
		expectErr      bool
		expectedErr    string
		recursionDepth int
	}{
		{
			name: "Success",
			setupMocks: func(client *vmock.Client, pub *publisher.MockPublisher) {
				client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
					[]valkey.ValkeyResult{
						vmock.Result(vmock.ValkeyString("OK")), // Result for SET
						vmock.Result(vmock.ValkeyInt64(1)),     // Result for XADD
					},
				)
				pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, event eventing.MdaiEvent, subject eventing.MdaiEventSubject) error {
						assert.Equal(t, "var.set", event.Name)
						assert.Equal(t, hubName, event.HubName)
						assert.Equal(t, correlationId, event.CorrelationID)
						assert.Equal(t, "eventhub", event.Source)
						assert.Equal(t, fmt.Sprintf("trigger.vars.set.%s.%s", hubName, variableKey), subject.String())

						var payload eventing.VariablesActionPayload
						err := json.Unmarshal([]byte(event.Payload), &payload)
						require.NoError(t, err)
						assert.Equal(t, variableKey, payload.VariableRef)
						assert.Equal(t, "string", payload.DataType)
						assert.Equal(t, "set", payload.Operation)
						assert.Equal(t, value, payload.Data)
						return nil
					})
			},
			expectErr: false,
		},
		{
			name: "Valkey command fails",
			setupMocks: func(client *vmock.Client, pub *publisher.MockPublisher) {
				client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
					[]valkey.ValkeyResult{
						vmock.Result(vmock.ValkeyError("valkey error")),
						vmock.Result(vmock.ValkeyInt64(0)),
					},
				)
			},
			expectErr:   true,
			expectedErr: "valkey error",
		},
		{
			name: "Publish fails",
			setupMocks: func(client *vmock.Client, pub *publisher.MockPublisher) {
				client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
					[]valkey.ValkeyResult{
						vmock.Result(vmock.ValkeyString("OK")),
						vmock.Result(vmock.ValkeyInt64(1)),
					},
				)
				pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).Return(errors.New("publish error")).AnyTimes()
			},
			expectErr:   true,
			expectedErr: "publish error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter, client, pub, ctrl := newAdapterWithMocks(t)
			defer ctrl.Finish()

			adapter.retryMaxTime = 0

			tc.setupMocks(client, pub)

			err := adapter.SetStringValue(ctx, variableKey, hubName, value, correlationId, 0)

			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMakeAuditEntry(t *testing.T) {
	testCases := []struct {
		caseName      string
		variableKey   string
		value         string
		correlationId string
		operation     string
		expected      StoreVariableAction
	}{
		{
			caseName:      "can make an audit entry",
			variableKey:   "foobar",
			value:         "barbaz",
			correlationId: "bazfoo",
			operation:     "DO ALL THE THINGS",
			expected: StoreVariableAction{
				Operation:     "DO ALL THE THINGS",
				Target:        "foobar",
				VariableRef:   "barbaz",
				Variable:      "barbaz",
				CorrelationId: "bazfoo",
			},
		},
	}

	for _, testCase := range testCases {
		t.Parallel()
		actual := makeAuditEntry(testCase.variableKey, testCase.value, testCase.correlationId, testCase.operation)
		assert.Equal(t, testCase.expected.Operation, actual.Operation)
		assert.Equal(t, testCase.expected.Target, actual.Target)
		assert.Equal(t, testCase.expected.VariableRef, actual.VariableRef)
		assert.Equal(t, testCase.expected.Variable, actual.Variable)
		assert.Equal(t, testCase.expected.CorrelationId, actual.CorrelationId)
	}
}

//nolint:goconst
func TestAddElementToSet_Success(t *testing.T) {
	ctx := context.Background()
	hub, key, value, corr := "hub-a", "var:set:tags", "green", "corr-1"

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0 // no retry path for this one

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)), // SADD
			vmock.Result(vmock.ValkeyInt64(1)), // XADD
		},
	)

	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, subj eventing.MdaiEventSubject) error {
			assert.Equal(t, "var.added", ev.Name)
			assert.Equal(t, hub, ev.HubName)
			assert.Equal(t, corr, ev.CorrelationID)
			assert.Equal(t, fmt.Sprintf("trigger.vars.added.%s.%s", hub, key), subj.String())

			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, key, pl.VariableRef)
			assert.Equal(t, "set", pl.DataType)
			assert.Equal(t, "added", pl.Operation)
			assert.Equal(t, value, pl.Data)
			return nil
		})

	err := adapter.AddElementToSet(ctx, key, hub, value, corr, 2)
	require.NoError(t, err)
}

func TestAddElementToSet_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	hub, key, value, corr := "hub-a", "var:set:tags", "blue", "corr-2"

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 300 * time.Millisecond

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)),
			vmock.Result(vmock.ValkeyInt64(1)),
		},
	)

	callCount := 0
	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, eventing.MdaiEvent, eventing.MdaiEventSubject) error {
			callCount++
			if callCount < 2 {
				return errors.New("transient publish")
			}
			return nil
		}).MinTimes(1)

	start := time.Now()
	err := adapter.AddElementToSet(ctx, key, hub, value, corr, 0)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
	// sanity: shouldn't take longer than the retryMaxTime by much
	assert.Less(t, elapsed, 2*adapter.retryMaxTime)
}

func TestRemoveElementFromSet_PublishError(t *testing.T) {
	ctx := context.Background()
	hub, key, value, corr := "hub-a", "var:set:tags", "red", "corr-3"

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)), // SREM
			vmock.Result(vmock.ValkeyInt64(1)), // XADD
		},
	)

	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		Return(errors.New("publish error"))

	err := adapter.RemoveElementFromSet(ctx, key, hub, value, corr, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish error")
}

func TestSetMapEntry_BehaviorAndPayload(t *testing.T) {
	ctx := context.Background()
	hub, key, field, value, corr := "hub-b", "var:map:settings", "mode", "auto", "corr-4"

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyString("OK")), // HSET
			vmock.Result(vmock.ValkeyInt64(1)),     // XADD
		},
	)

	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, subj eventing.MdaiEventSubject) error {
			assert.Equal(t, "var.set", ev.Name)
			assert.Equal(t, fmt.Sprintf("trigger.vars.set.%s.%s", hub, key), subj.String())
			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, "map", pl.DataType)
			assert.Equal(t, "set", pl.Operation)
			// NOTE: current implementation passes 'value' (not a {"field","value"} object)
			assert.Equal(t, value, pl.Data)
			return nil
		})

	err := adapter.SetMapEntry(ctx, key, hub, field, value, corr, 0)
	require.NoError(t, err)
}

func TestRemoveMapEntry_BehaviorAndPayload(t *testing.T) {
	ctx := context.Background()
	hub, key, field, corr := "hub-b", "var:map:settings", "obsolete", "corr-5"

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)), // HDEL
			vmock.Result(vmock.ValkeyInt64(1)), // XADD
		},
	)

	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, subj eventing.MdaiEventSubject) error {
			assert.Equal(t, "var.remove", ev.Name)
			assert.Equal(t, fmt.Sprintf("trigger.vars.remove.%s.%s", hub, key), subj.String())
			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, "map", pl.DataType)
			assert.Equal(t, "remove", pl.Operation)
			// current implementation passes 'field'
			assert.Equal(t, field, pl.Data)
			return nil
		})

	err := adapter.RemoveMapEntry(ctx, key, hub, field, corr, 0)
	require.NoError(t, err)
}

func TestAccumulateErrors_MultipleAreAggregated(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := vmock.NewClient(ctrl)
	pub := publisher.NewMockPublisher(ctrl)
	adapter := NewHandlerAdapter(client, logger, pub)

	results := []valkey.ValkeyResult{
		vmock.Result(vmock.ValkeyError("first failure")),
		vmock.Result(vmock.ValkeyError("second failure")),
	}

	err := adapter.accumulateErrors(results, "var:key")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "first failure")
	assert.Contains(t, err.Error(), "second failure")
	assert.Contains(t, err.Error(), "var:key")
}

func TestRetryWithBackoff_SucceedsAfterRetries(t *testing.T) {
	ctx := context.Background()
	failures := 2
	calls := 0

	err := retryWithBackoff(ctx, func() error {
		calls++
		if calls <= failures {
			return errors.New("not yet")
		}
		return nil
	}, 800*time.Millisecond)

	require.NoError(t, err)
	assert.Equal(t, failures+1, calls)
}

func TestRetryWithBackoff_NoRetryWhenMaxZero(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := retryWithBackoff(ctx, func() error {
		calls++
		return errors.New("always")
	}, 0)

	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryWithBackoff_TimesOut(t *testing.T) {
	ctx := context.Background()
	start := time.Now()

	err := retryWithBackoff(ctx, func() error {
		return errors.New("still failing")
	}, 120*time.Millisecond)

	elapsed := time.Since(start)
	require.Error(t, err)
	// should be roughly around the max window (allow jitter)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
}

func TestPublishVarUpdate_BuildsEventAndSubject(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := vmock.NewClient(ctrl)
	pub := publisher.NewMockPublisher(ctrl)
	adapter := NewHandlerAdapter(client, logger, pub)

	hub, varName, varType, action, data, corr := "hub-z", "var:foo", "string", "set", "abc", "c-9"
	recDepth := 7

	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, subj eventing.MdaiEventSubject) error {
			assert.Equal(t, "var.set", ev.Name)
			assert.Equal(t, hub, ev.HubName)
			assert.Equal(t, "eventhub", ev.Source)
			assert.Equal(t, corr, ev.CorrelationID)
			assert.Equal(t, recDepth, ev.RecursionDepth)
			assert.Equal(t, fmt.Sprintf("trigger.vars.set.%s.%s", hub, varName), subj.String())

			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, varName, pl.VariableRef)
			assert.Equal(t, varType, pl.DataType)
			assert.Equal(t, action, pl.Operation)
			assert.Equal(t, data, pl.Data)
			return nil
		})

	err := adapter.publishVarUpdate(ctx, PublishVarUpdateParams{
		Hub:            hub,
		VarName:        varName,
		VarType:        varType,
		Action:         action,
		Data:           data,
		CorrelationID:  corr,
		Source:         "eventhub",
		RecursionDepth: recDepth,
	})
	require.NoError(t, err)
}

func TestStoreVariableAction_ToSequence_FieldsPresent(t *testing.T) {
	action := StoreVariableAction{
		HubName:        "hub-q",
		EventId:        "e-1",
		Operation:      "op",
		Target:         "tgt",
		VariableRef:    "ref",
		Variable:       "val",
		CorrelationId:  "corr",
		RecursionDepth: 3,
	}

	// Collect yielded pairs into a map
	got := map[string]string{}
	for k, v := range action.ToSequence() {
		got[k] = v
	}

	// expected non-empty keys
	for _, k := range []string{
		"timestamp", "hub_name", "event_id", "operation",
		"target", "variable_ref", "variable", "correlation_id", "recursion_depth",
	} {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing key %q in sequence", k)
		}
	}

	assert.Equal(t, "hub-q", got["hub_name"])
	assert.Equal(t, "e-1", got["event_id"])
	assert.Equal(t, "op", got["operation"])
	assert.Equal(t, "tgt", got["target"])
	assert.Equal(t, "ref", got["variable_ref"])
	assert.Equal(t, "val", got["variable"])
	assert.Equal(t, "corr", got["correlation_id"])
	assert.Equal(t, strconv.Itoa(3), got["recursion_depth"])
	assert.NotEmpty(t, got["timestamp"])
}
