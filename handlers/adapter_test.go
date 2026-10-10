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
	"github.com/mydecisive/mdai-data-core/variables"
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
	ctx := t.Context()

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

func TestDeleteStringValue(t *testing.T) {
	hubName := "my-hub"
	variableKey := "my-var"
	correlationId := "corr-id-123"
	ctx := t.Context()

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)), // DEL: 1 key removed
			vmock.Result(vmock.ValkeyInt64(1)), // XADD
		},
	)
	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, event eventing.MdaiEvent, subject eventing.MdaiEventSubject) error {
			assert.Equal(t, "var.removed", event.Name)
			assert.Equal(t, hubName, event.HubName)
			assert.Equal(t, correlationId, event.CorrelationID)
			assert.Equal(t, fmt.Sprintf("trigger.vars.removed.%s.%s", hubName, variableKey), subject.String())

			var payload eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(event.Payload), &payload))
			assert.Equal(t, variableKey, payload.VariableRef)
			assert.Equal(t, "string", payload.DataType)
			assert.Equal(t, "removed", payload.Operation)
			assert.Empty(t, payload.Data)
			return nil
		})

	require.NoError(t, adapter.DeleteStringValue(ctx, variableKey, hubName, correlationId, 0))
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
	ctx := t.Context()
	const (
		hub   = "hub-a"
		key   = "var:set:tags"
		value = "green"
		corr  = "corr-1"
	)

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
	ctx := t.Context()
	const (
		hub   = "hub-a"
		key   = "var:set:tags"
		value = "blue"
		corr  = "corr-2"
	)

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = time.Second // room for three attempts

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyInt64(1)),
			vmock.Result(vmock.ValkeyInt64(1)),
		},
	)

	callCount := 0
	var eventIDs []string
	// Each attempt gets a context derived from ctx and bounded by retryMaxTime, so match any context.
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(attemptCtx context.Context, ev eventing.MdaiEvent, _ eventing.MdaiEventSubject) error {
			_, hasDeadline := attemptCtx.Deadline()
			assert.True(t, hasDeadline, "each attempt should be bounded by retryMaxTime")
			eventIDs = append(eventIDs, ev.ID)
			callCount++
			if callCount < 3 {
				return errors.New("transient publish")
			}
			return nil
		}).MinTimes(1)

	start := time.Now()
	err := adapter.AddElementToSet(ctx, key, hub, value, corr, 0)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 3, callCount)
	// The event is built once, so JetStream can deduplicate retries by Nats-Msg-Id.
	require.Len(t, eventIDs, 3)
	assert.NotEmpty(t, eventIDs[0])
	assert.Equal(t, eventIDs[0], eventIDs[1], "retries must reuse the event ID")
	assert.Equal(t, eventIDs[0], eventIDs[2], "retries must reuse the event ID")
	// sanity: shouldn't take longer than the retryMaxTime by much
	assert.Less(t, elapsed, 2*adapter.retryMaxTime)
}

func TestRemoveElementFromSet_PublishError(t *testing.T) {
	ctx := t.Context()
	const (
		hub   = "hub-a"
		key   = "var:set:tags"
		value = "red"
		corr  = "corr-3"
	)

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
	ctx := t.Context()
	const (
		hub   = "hub-b"
		key   = "var:map:settings"
		field = "mode"
		value = "auto"
		corr  = "corr-4"
	)

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
	ctx := t.Context()
	const (
		hub   = "hub-b"
		key   = "var:map:settings"
		field = "obsolete"
		corr  = "corr-5"
	)

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
			assert.Equal(t, "var.removed", ev.Name)
			assert.Equal(t, fmt.Sprintf("trigger.vars.removed.%s.%s", hub, key), subj.String())
			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, "map", pl.DataType)
			assert.Equal(t, "removed", pl.Operation)
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
	ctx := t.Context()
	failures := 2
	calls := 0

	err := retryWithBackoff(ctx, func(context.Context) error {
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
	ctx := t.Context()
	calls := 0
	err := retryWithBackoff(ctx, func(context.Context) error {
		calls++
		return errors.New("always")
	}, 0)

	require.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryWithBackoff_TimesOut(t *testing.T) {
	ctx := t.Context()
	start := time.Now()

	err := retryWithBackoff(ctx, func(context.Context) error {
		return errors.New("still failing")
	}, 120*time.Millisecond)

	elapsed := time.Since(start)
	require.Error(t, err)
	// should be roughly around the max window (allow jitter)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	// The deadline error and the last attempt's error are both kept.
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "still failing")
}

func TestBuildVarUpdate_BuildsEventAndSubject(t *testing.T) {
	const (
		hub     = "hub-z"
		varName = "var:foo"
		action  = "set"
		data    = "abc"
		corr    = "c-9"
	)
	varType := variables.DataTypeString
	recDepth := 7

	ev, subj, err := buildVarUpdate(PublishVarUpdateParams{
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

	assert.Equal(t, "var.set", ev.Name)
	assert.Equal(t, hub, ev.HubName)
	assert.Equal(t, "eventhub", ev.Source)
	assert.Equal(t, corr, ev.CorrelationID)
	assert.Equal(t, recDepth, ev.RecursionDepth)
	assert.NotEmpty(t, ev.ID, "defaults should be applied once, at build time")
	assert.False(t, ev.Timestamp.IsZero())
	assert.Equal(t, fmt.Sprintf("trigger.vars.set.%s.%s", hub, varName), subj.String())

	var pl eventing.VariablesActionPayload
	require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
	assert.Equal(t, varName, pl.VariableRef)
	assert.Equal(t, string(varType), pl.DataType)
	assert.Equal(t, action, pl.Operation)
	assert.Equal(t, data, pl.Data)
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

func TestMutation_SanitizesSubjectTokens(t *testing.T) {
	ctx := t.Context()
	const (
		hub   = "hub.x"
		key   = "a.b *>c\fd"
		value = "v"
		corr  = "corr-s"
	)

	adapter, client, pub, ctrl := newAdapterWithMocks(t)
	defer ctrl.Finish()
	adapter.retryMaxTime = 0

	client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
		[]valkey.ValkeyResult{
			vmock.Result(vmock.ValkeyString("OK")),
			vmock.Result(vmock.ValkeyInt64(1)),
		},
	)
	pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, subj eventing.MdaiEventSubject) error {
			// Exactly three tokens after trigger.vars, so the subject matches the stream.
			assert.Equal(t, "trigger.vars.set.hub_x.a_b___c_d", subj.String())
			assert.Equal(t, hub, ev.HubName, "the event keeps the original hub name")

			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
			assert.Equal(t, key, pl.VariableRef, "the payload keeps the original variable name")
			return nil
		})

	require.NoError(t, adapter.SetStringValue(ctx, key, hub, value, corr, 0))
}

func TestMutation_InvalidEventFailsBeforeValkey(t *testing.T) {
	cases := []struct {
		name, hub, key, wantErr string
	}{
		{name: "missing hub", hub: "", key: "my-var", wantErr: "hubName"},
		{name: "missing variable", hub: "my-hub", key: "", wantErr: "variable name is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter, _, _, ctrl := newAdapterWithMocks(t)
			defer ctrl.Finish()
			// No DoMulti or Publish expectations: gomock fails the test if either is called.

			err := adapter.SetStringValue(t.Context(), tc.key, tc.hub, "v", "corr", 0)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestScalarValue_PublishesDataType(t *testing.T) {
	for _, dataType := range []variables.DataType{
		variables.DataTypeString, variables.DataTypeInt, variables.DataTypeFloat, variables.DataTypeBoolean,
	} {
		t.Run(string(dataType), func(t *testing.T) {
			ctx := t.Context()
			adapter, client, pub, ctrl := newAdapterWithMocks(t)
			defer ctrl.Finish()
			adapter.retryMaxTime = 0

			client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).Return(
				[]valkey.ValkeyResult{
					vmock.Result(vmock.ValkeyString("OK")),
					vmock.Result(vmock.ValkeyInt64(1)),
				},
			).Times(2)

			var gotTypes []string
			pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, _ eventing.MdaiEventSubject) error {
					var pl eventing.VariablesActionPayload
					require.NoError(t, json.Unmarshal([]byte(ev.Payload), &pl))
					gotTypes = append(gotTypes, pl.Operation+":"+pl.DataType)
					return nil
				}).Times(2)

			require.NoError(t, adapter.SetScalarValue(ctx, "my-var", "my-hub", dataType, "1", "corr", 0))
			require.NoError(t, adapter.DeleteScalarValue(ctx, "my-var", "my-hub", dataType, "corr", 0))
			assert.Equal(t, []string{"set:" + string(dataType), "removed:" + string(dataType)}, gotTypes)
		})
	}
}

func TestScalarValue_RejectsNonScalarTypes(t *testing.T) {
	for _, dataType := range []variables.DataType{
		variables.DataTypeSet, variables.DataTypeMap, variables.DataTypeMetaHashSet, variables.DataTypeMetaPriorityList, "bogus",
	} {
		t.Run(string(dataType), func(t *testing.T) {
			adapter, _, _, ctrl := newAdapterWithMocks(t)
			defer ctrl.Finish()
			// No DoMulti or Publish expectations: nothing may be written or published.

			err := adapter.SetScalarValue(t.Context(), "my-var", "my-hub", dataType, "1", "corr", 0)
			require.ErrorIs(t, err, variables.ErrUnsupportedDataType)

			err = adapter.DeleteScalarValue(t.Context(), "my-var", "my-hub", dataType, "corr", 0)
			require.ErrorIs(t, err, variables.ErrUnsupportedDataType)
		})
	}
}

func TestRetryWithBackoff_BoundsEachAttempt(t *testing.T) {
	start := time.Now()

	// An attempt that never finishes on its own must still end when retryMaxTime passes.
	err := retryWithBackoff(t.Context(), func(attemptCtx context.Context) error {
		<-attemptCtx.Done()
		return errors.New("publish blocked")
	}, 150*time.Millisecond)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestRetryWithBackoff_CallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(50*time.Millisecond, cancel)

	err := retryWithBackoff(ctx, func(context.Context) error {
		return errors.New("still failing")
	}, 5*time.Second)

	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "still failing", "the last publish error should be kept")
}

// auditFields returns the field/value pairs of the XADD audit command sent with a mutation.
func auditFields(t *testing.T, cmds []valkey.Completed) map[string]string {
	t.Helper()
	require.Len(t, cmds, 2, "expected the variable update and the audit XADD")
	args := cmds[1].Commands()
	// XADD <stream> MINID <threshold> * field value [field value ...]
	require.GreaterOrEqual(t, len(args), 5)
	require.Equal(t, "XADD", args[0])
	fields := map[string]string{}
	for i := 5; i+1 < len(args); i += 2 {
		fields[args[i]] = args[i+1]
	}
	return fields
}

func TestMutation_AuditEntryMatchesPublishedEvent(t *testing.T) {
	const (
		hub   = "my-hub"
		key   = "my-var"
		corr  = "corr-a"
		depth = 3
	)

	cases := []struct {
		name          string
		mutate        func(ctx context.Context, a *HandlerAdapter) error
		wantOperation string
		wantField     string
	}{
		{
			name: "set entry",
			mutate: func(ctx context.Context, a *HandlerAdapter) error {
				return a.AddElementToSet(ctx, key, hub, "blue", corr, depth)
			},
			wantOperation: "Add element to set",
		},
		{
			name: "map set",
			mutate: func(ctx context.Context, a *HandlerAdapter) error {
				return a.SetMapEntry(ctx, key, hub, "color", "blue", corr, depth)
			},
			wantOperation: "Set map entry",
			wantField:     "color",
		},
		{
			name: "map remove",
			mutate: func(ctx context.Context, a *HandlerAdapter) error {
				return a.RemoveMapEntry(ctx, key, hub, "color", corr, depth)
			},
			wantOperation: "Remove map entry",
			wantField:     "color",
		},
		{
			name: "int scalar",
			mutate: func(ctx context.Context, a *HandlerAdapter) error {
				return a.SetScalarValue(ctx, key, hub, variables.DataTypeInt, "7", corr, depth)
			},
			wantOperation: "Set int value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			adapter, client, pub, ctrl := newAdapterWithMocks(t)
			defer ctrl.Finish()
			adapter.retryMaxTime = 0

			var audit map[string]string
			client.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
					audit = auditFields(t, cmds)
					return []valkey.ValkeyResult{
						vmock.Result(vmock.ValkeyInt64(1)),
						vmock.Result(vmock.ValkeyString("1-0")),
					}
				})

			var published eventing.MdaiEvent
			pub.EXPECT().Publish(ctx, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, ev eventing.MdaiEvent, _ eventing.MdaiEventSubject) error {
					published = ev
					return nil
				})

			require.NoError(t, tc.mutate(ctx, adapter))

			require.NotEmpty(t, published.ID)
			assert.Equal(t, published.ID, audit["event_id"], "the audit entry should reference the published event")
			assert.Equal(t, hub, audit["hub_name"])
			assert.Equal(t, strconv.Itoa(depth), audit["recursion_depth"])
			assert.Equal(t, corr, audit["correlation_id"])
			assert.Equal(t, key, audit["target"])
			assert.Equal(t, tc.wantOperation, audit["operation"])

			var pl eventing.VariablesActionPayload
			require.NoError(t, json.Unmarshal([]byte(published.Payload), &pl))
			if tc.wantField == "" {
				assert.NotContains(t, audit, "field")
				assert.Empty(t, pl.Field)
			} else {
				assert.Equal(t, tc.wantField, audit["field"])
				assert.Equal(t, tc.wantField, pl.Field)
			}
		})
	}
}

func TestNewHandlerAdapter_AuditStreamRetention(t *testing.T) {
	newAdapter := func(t *testing.T, opts ...variables.ValkeyAdapterOption) *HandlerAdapter {
		t.Helper()
		ctrl := gomock.NewController(t)
		return NewHandlerAdapter(vmock.NewClient(ctrl), zap.NewNop(), publisher.NewMockPublisher(ctrl), opts...)
	}

	t.Run("from env", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "72h")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
		assert.Equal(t, 72*time.Hour, newAdapter(t).valkeyAdapter.AuditStreamExpiry())
	})

	t.Run("explicit option overrides env", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "72h")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
		adapter := newAdapter(t, variables.WithValkeyAuditStreamExpiry(time.Hour))
		assert.Equal(t, time.Hour, adapter.valkeyAdapter.AuditStreamExpiry())
	})

	t.Run("invalid env falls back to default", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "30 days")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
		assert.Equal(t, 30*24*time.Hour, newAdapter(t).valkeyAdapter.AuditStreamExpiry())
	})

	t.Run("unset uses default", func(t *testing.T) {
		t.Setenv("VALKEY_AUDIT_STREAM_RETENTION", "")
		t.Setenv("VALKEY_AUDIT_STREAM_EXPIRY_MS", "")
		assert.Equal(t, 30*24*time.Hour, newAdapter(t).valkeyAdapter.AuditStreamExpiry())
	})
}
