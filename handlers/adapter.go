package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/mydecisive/mdai-data-core/audit"
	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/eventing/config"
	"github.com/mydecisive/mdai-data-core/eventing/publisher"
	"github.com/mydecisive/mdai-data-core/variables"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	source        = "eventhub"
	actionAdded   = "added"
	actionSet     = "set"
	actionRemoved = "removed"
)

// HandlerAdapter is a wrapper for handling variable operations.
// Functions of HandlerAdapter execute the provided valkey commands and logs audit entries.
// Common functions arguments:
// ctx is the context for the request.
// variableKey is the key for the Valkey data type.
// hubName is the name of the associated hub, used for namespacing.
// value is the element to be added to the data type.
// correlationId is used for tracking and auditing purposes.
type HandlerAdapter struct {
	client        valkey.Client
	logger        *zap.Logger
	valkeyAdapter *variables.ValkeyAdapter
	publisher     publisher.Publisher
	retryMaxTime  time.Duration
}

// NewHandlerAdapter creates a new wrapper for handling variable operations.
func NewHandlerAdapter(client valkey.Client, logger *zap.Logger, pub publisher.Publisher, opts ...variables.ValkeyAdapterOption) *HandlerAdapter {
	va := variables.NewValkeyAdapter(client, logger, opts...)

	ha := &HandlerAdapter{
		client:        client,
		logger:        logger,
		valkeyAdapter: va,
		publisher:     pub,
		retryMaxTime:  10 * time.Second,
	}

	return ha
}

// AddElementToSet adds an element to a Set data type and logs an audit entry.
func (r *HandlerAdapter) AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.AddElementToSet(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Add element to set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        variables.DataTypeSet,
		Action:         actionAdded,
		Data:           value,
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// RemoveElementFromSet removes an element from a Set data type and logs an audit entry.
func (r *HandlerAdapter) RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveElementFromSet(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        variables.DataTypeSet,
		Action:         actionRemoved,
		Data:           value,
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// SetMapEntry sets a field-value pair in a map data type and logs an audit entry.
// It is using Valkey's HSET command on a Hash data type under the hood.
// This command overwrites the values of specified fields that exist in the hash.
// If key doesn't exist, a new key holding a hash is created.
func (r *HandlerAdapter) SetMapEntry(ctx context.Context, variableKey string, hubName string, field string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.SetMapEntry(variableKey, hubName, field, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Set map entry")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        variables.DataTypeMap,
		Action:         actionSet,
		Data:           value,
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// RemoveMapEntry removes a field from a map data type and logs an audit entry.
// It uses Valkey's HDEL command on a Hash data type under the hood.
func (r *HandlerAdapter) RemoveMapEntry(ctx context.Context, variableKey string, hubName string, field string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveMapEntry(variableKey, hubName, field)

	auditEntry := makeAuditEntry(variableKey, field, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        variables.DataTypeMap,
		Action:         actionRemoved,
		Data:           field,
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// SetStringValue sets a string value and logs an audit entry.
// It is SetScalarValue with variables.DataTypeString.
func (r *HandlerAdapter) SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationId string, recursionDepth int) error {
	return r.SetScalarValue(ctx, variableKey, hubName, variables.DataTypeString, value, correlationId, recursionDepth)
}

// SetScalarValue sets a scalar (string, int, float or boolean) value and logs an audit entry.
// The published event carries dataType, so consumers can tell the scalar types apart.
// value is stored as given; pass the canonical form for dataType (see variables.CanonicalizeScalar).
// It returns an error wrapping variables.ErrUnsupportedDataType if dataType is not a scalar type.
func (r *HandlerAdapter) SetScalarValue(ctx context.Context, variableKey string, hubName string, dataType variables.DataType, value string, correlationId string, recursionDepth int) error {
	if err := requireScalar(dataType); err != nil {
		return err
	}

	variableUpdateCommand := r.valkeyAdapter.SetString(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, fmt.Sprintf("Set %s value", dataType))
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        dataType,
		Action:         actionSet,
		Data:           value,
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// DeleteStringValue removes the stored scalar so subsequent reads fall back to
// the declared default (or report not-found). Used by the gateway's DELETE path.
// It is DeleteScalarValue with variables.DataTypeString.
func (r *HandlerAdapter) DeleteStringValue(ctx context.Context, variableKey string, hubName string, correlationId string, recursionDepth int) error {
	return r.DeleteScalarValue(ctx, variableKey, hubName, variables.DataTypeString, correlationId, recursionDepth)
}

// DeleteScalarValue removes a stored scalar (string, int, float or boolean) so subsequent reads
// fall back to the declared default (or report not-found), and logs an audit entry.
// The published event carries dataType. It returns an error wrapping
// variables.ErrUnsupportedDataType if dataType is not a scalar type.
func (r *HandlerAdapter) DeleteScalarValue(ctx context.Context, variableKey string, hubName string, dataType variables.DataType, correlationId string, recursionDepth int) error {
	if err := requireScalar(dataType); err != nil {
		return err
	}

	variableUpdateCommand := r.valkeyAdapter.DeleteString(variableKey, hubName)

	auditEntry := makeAuditEntry(variableKey, "", correlationId, fmt.Sprintf("Delete %s value", dataType))
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	return r.applyAndPublish(ctx, variableKey, variableUpdateCommand, auditLogCommand, PublishVarUpdateParams{
		Hub:            hubName,
		VarName:        variableKey,
		VarType:        dataType,
		Action:         actionRemoved,
		Data:           "",
		CorrelationID:  correlationId,
		Source:         source,
		RecursionDepth: recursionDepth,
	})
}

// requireScalar returns an error wrapping variables.ErrUnsupportedDataType unless dataType is a scalar type.
func requireScalar(dataType variables.DataType) error {
	switch dataType {
	case variables.DataTypeString, variables.DataTypeInt, variables.DataTypeFloat, variables.DataTypeBoolean:
		return nil
	default:
		return fmt.Errorf("%w: %q is not a scalar type", variables.ErrUnsupportedDataType, dataType)
	}
}

// applyAndPublish runs the mutation workflow: it builds and validates the variable-update
// event, sends the Valkey update and audit entry, then publishes the event with retries.
// The event is built once, before Valkey is touched, so an invalid event fails without
// side effects and every publish attempt carries the same event ID (sent as Nats-Msg-Id).
// That lets JetStream deduplicate a retry of a publish the server had already stored.
func (r *HandlerAdapter) applyAndPublish(
	ctx context.Context,
	variableKey string,
	variableUpdateCommand valkey.Completed,
	auditLogCommand valkey.Completed,
	params PublishVarUpdateParams,
) error {
	event, subject, err := buildVarUpdate(params)
	if err != nil {
		return err
	}

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}

	return retryWithBackoff(ctx, func(ctx context.Context) error {
		return r.publisher.Publish(ctx, event, subject)
	}, r.retryMaxTime)
}

func (r *HandlerAdapter) executeAuditedUpdateCommand(ctx context.Context, variableKey string, variableUpdateCommand valkey.Completed, auditLogCommand valkey.Completed) error {
	results := r.client.DoMulti(
		ctx,
		variableUpdateCommand,
		auditLogCommand,
	)

	return r.accumulateErrors(results, variableKey)
}

func (r *HandlerAdapter) accumulateErrors(results []valkey.ValkeyResult, key string) error {
	var errs []string
	for _, result := range results {
		if result.Error() != nil {
			errs = append(errs, fmt.Sprintf("operation failed on key %s: %s", key, result.Error()))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

// retryWithBackoff calls fn until it succeeds or maxElapsed passes; a non-positive maxElapsed
// means a single attempt. Each attempt gets a context bounded by maxElapsed. If it gives up,
// the error wraps both the context error and the last error from fn.
func retryWithBackoff(ctx context.Context, fn func(context.Context) error, maxElapsed time.Duration) error {
	if maxElapsed <= 0 {
		return fn(ctx)
	}

	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 100 * time.Millisecond
	backOff.Multiplier = 2.0
	backOff.MaxInterval = 2 * time.Second

	ctx, cancel := context.WithTimeout(ctx, maxElapsed)
	defer cancel()

	var lastErr error
	operation := func() (bool, error) {
		if err := fn(ctx); err != nil {
			lastErr = err
			return false, err
		}
		return true, nil
	}

	_, err := backoff.Retry(ctx, operation, backoff.WithBackOff(backOff))
	if err != nil && lastErr != nil && !errors.Is(err, lastErr) {
		// backoff.Retry returns only the context error once ctx is done; keep the cause.
		return errors.Join(err, lastErr)
	}
	return err
}

type PublishVarUpdateParams struct {
	Hub            string
	VarName        string
	VarType        variables.DataType
	Action         string // "added" | "removed" | "set"
	Data           any    // value or {"field":..., "value":...} or {"field":...} for remove
	CorrelationID  string
	Source         string // e.g. "eventhub" or your worker name
	RecursionDepth int
}

var errMissingVariableName = errors.New("variable name is required")

// buildVarUpdate builds and validates the variable-update event and its subject.
// The subject is trigger.vars.<action>.<hub>.<variable>; hub and variable names are passed
// through config.SafeToken so each is exactly one subject token. The payload keeps the
// original variable name.
func buildVarUpdate(params PublishVarUpdateParams) (eventing.MdaiEvent, eventing.MdaiEventSubject, error) {
	if params.VarName == "" {
		return eventing.MdaiEvent{}, eventing.MdaiEventSubject{}, errMissingVariableName
	}

	pl := eventing.VariablesActionPayload{
		VariableRef: params.VarName,
		DataType:    string(params.VarType),
		Operation:   params.Action,
		Data:        params.Data,
	}
	plb, err := json.Marshal(pl)
	if err != nil {
		return eventing.MdaiEvent{}, eventing.MdaiEventSubject{}, fmt.Errorf("marshal variable update payload: %w", err)
	}

	ev := eventing.MdaiEvent{
		Name:           "var." + params.Action,
		Version:        1,
		HubName:        params.Hub,
		Source:         params.Source,
		CorrelationID:  params.CorrelationID,
		RecursionDepth: params.RecursionDepth,
		Payload:        string(plb),
	}
	ev.ApplyDefaults()
	if err := ev.Validate(); err != nil {
		return eventing.MdaiEvent{}, eventing.MdaiEventSubject{}, fmt.Errorf("invalid variable update event: %w", err)
	}

	subj := eventing.NewMdaiEventSubject(eventing.TriggerEventType,
		strings.Join([]string{params.Action, config.SafeToken(params.Hub), config.SafeToken(params.VarName)}, "."))

	return ev, subj, nil
}

func makeAuditEntry(variableKey string, value string, correlationId string, operation string) StoreVariableAction {
	auditAction := StoreVariableAction{
		EventId:       time.Now().String(),
		Operation:     operation,
		Target:        variableKey,
		VariableRef:   value,
		Variable:      value,
		CorrelationId: correlationId,
	}
	return auditAction
}

func (r *HandlerAdapter) makeVariableAuditLogActionCommand(action StoreVariableAction) valkey.Completed {
	return r.client.B().Xadd().Key(audit.MdaiHubEventHistoryStreamName).Minid().
		Threshold(audit.GetAuditLogTTLMinId(r.valkeyAdapter.AuditStreamExpiry())).
		Id("*").FieldValue().FieldValueIter(action.ToSequence()).
		Build()
}

type StoreVariableAction struct {
	HubName        string `json:"hub_name"`
	EventId        string `json:"event_id"`
	Operation      string `json:"operation"`
	Target         string `json:"target"`
	VariableRef    string `json:"variable_ref"`
	Variable       string `json:"variable"`
	CorrelationId  string `json:"correlation_id"`
	RecursionDepth int    `json:"recursion_depth"`
}

func (action StoreVariableAction) ToSequence() iter.Seq2[string, string] {
	return func(yield func(K string, V string) bool) {
		fields := map[string]string{
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
			"hub_name":        action.HubName,
			"event_id":        action.EventId,
			"operation":       action.Operation,
			"target":          action.Target,
			"variable_ref":    action.VariableRef,
			"variable":        action.Variable,
			"correlation_id":  action.CorrelationId,
			"recursion_depth": strconv.Itoa(action.RecursionDepth),
		}

		for key, value := range fields {
			if value == "" {
				continue
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
