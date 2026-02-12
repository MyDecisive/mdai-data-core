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
	"github.com/mydecisive/mdai-data-core/eventing/publisher"
	variables "github.com/mydecisive/mdai-data-core/variables"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	source         = "eventhub"
	actionAdded    = "added"
	actionSet      = "set"
	actionRemove   = "remove"
	dataTypeString = "string"
	dataTypeMap    = "map"
	dataTypeSet    = "set"
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

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}

	return retryWithBackoff(ctx, func() error {
		return r.publishVarUpdate(ctx, PublishVarUpdateParams{
			Hub:            hubName,
			VarName:        variableKey,
			VarType:        dataTypeSet,
			Action:         actionAdded,
			Data:           value,
			CorrelationID:  correlationId,
			Source:         source,
			RecursionDepth: recursionDepth,
		})
	}, r.retryMaxTime)
}

// RemoveElementFromSet removes an element from a Set data type and logs an audit entry.
func (r *HandlerAdapter) RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveElementFromSet(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}

	return retryWithBackoff(ctx, func() error {
		return r.publishVarUpdate(ctx, PublishVarUpdateParams{
			Hub:            hubName,
			VarName:        variableKey,
			VarType:        dataTypeSet,
			Action:         actionRemove,
			Data:           value,
			CorrelationID:  correlationId,
			Source:         source,
			RecursionDepth: recursionDepth,
		})
	}, r.retryMaxTime)
}

// SetMapEntry sets a field-value pair in a map data type and logs an audit entry.
// It is using Valkey's HSET command on a Hash data type under the hood.
// This command overwrites the values of specified fields that exist in the hash.
// If key doesn't exist, a new key holding a hash is created.
func (r *HandlerAdapter) SetMapEntry(ctx context.Context, variableKey string, hubName string, field string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.SetMapEntry(variableKey, hubName, field, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Set map entry")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}
	return retryWithBackoff(ctx, func() error {
		return r.publishVarUpdate(ctx, PublishVarUpdateParams{
			Hub:            hubName,
			VarName:        variableKey,
			VarType:        dataTypeMap,
			Action:         actionSet,
			Data:           value,
			CorrelationID:  correlationId,
			Source:         source,
			RecursionDepth: recursionDepth,
		})
	}, r.retryMaxTime)
}

// RemoveMapEntry removes a field from a map data type and logs an audit entry.
// It uses Valkey's HDEL command on a Hash data type under the hood.
func (r *HandlerAdapter) RemoveMapEntry(ctx context.Context, variableKey string, hubName string, field string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.RemoveMapEntry(variableKey, hubName, field)

	auditEntry := makeAuditEntry(variableKey, field, correlationId, "Remove element from set")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}
	return retryWithBackoff(ctx, func() error {
		return r.publishVarUpdate(ctx, PublishVarUpdateParams{
			Hub:            hubName,
			VarName:        variableKey,
			VarType:        dataTypeMap,
			Action:         actionRemove,
			Data:           field,
			CorrelationID:  correlationId,
			Source:         source,
			RecursionDepth: recursionDepth,
		})
	}, r.retryMaxTime)
}

// SetStringValue sets a string value and logs an audit entry.
func (r *HandlerAdapter) SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationId string, recursionDepth int) error {
	variableUpdateCommand := r.valkeyAdapter.SetString(variableKey, hubName, value)

	auditEntry := makeAuditEntry(variableKey, value, correlationId, "Set string value")
	auditLogCommand := r.makeVariableAuditLogActionCommand(auditEntry)

	if err := r.executeAuditedUpdateCommand(ctx, variableKey, variableUpdateCommand, auditLogCommand); err != nil {
		return err
	}
	return retryWithBackoff(ctx, func() error {
		return r.publishVarUpdate(ctx, PublishVarUpdateParams{
			Hub:            hubName,
			VarName:        variableKey,
			VarType:        dataTypeString,
			Action:         actionSet,
			Data:           value,
			CorrelationID:  correlationId,
			Source:         source,
			RecursionDepth: recursionDepth,
		})
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

func retryWithBackoff(ctx context.Context, fn func() error, maxElapsed time.Duration) error {
	if maxElapsed <= 0 {
		return fn()
	}

	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 100 * time.Millisecond
	backOff.Multiplier = 2.0
	backOff.MaxInterval = 2 * time.Second

	ctx, cancel := context.WithTimeout(ctx, maxElapsed)
	defer cancel()

	operation := func() (bool, error) {
		if err := fn(); err != nil {
			select {
			case <-ctx.Done():
				return false, backoff.Permanent(ctx.Err())
			default:
			}
			return false, err
		}
		return true, nil
	}

	_, err := backoff.Retry(ctx, operation, backoff.WithBackOff(backOff))
	return err
}

type PublishVarUpdateParams struct {
	Hub            string
	VarName        string
	VarType        string // "set" | "map" | "string" | "int" | "boolean" | ...
	Action         string // "added" | "removed" | "set"
	Data           any    // value or {"field":..., "value":...} or {"field":...} for remove
	CorrelationID  string
	Source         string // e.g. "eventhub" or your worker name
	RecursionDepth int
}

//nolint:unparam
func (r *HandlerAdapter) publishVarUpdate(ctx context.Context, params PublishVarUpdateParams) error {
	pl := eventing.VariablesActionPayload{
		VariableRef: params.VarName,
		DataType:    params.VarType,
		Operation:   params.Action,
		Data:        params.Data,
	}
	plb, _ := json.Marshal(pl)

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

	subj := eventing.NewMdaiEventSubject(eventing.TriggerEventType, fmt.Sprintf("%s.%s.%s", params.Action, params.Hub, params.VarName))

	return r.publisher.Publish(ctx, ev, subj)
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
