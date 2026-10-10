package variables

import (
	"context"
	"fmt"
	"math"

	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/valkey-io/valkey-go"
	"gopkg.in/yaml.v3"
)

const (
	variableKeyPrefix = "variable/"
	// ValkeyWrongTypeOrNotFoundError is returned by custom Valkey module commands
	// when the key is missing or has an incompatible type.
	ValkeyWrongTypeOrNotFoundError = "WRONGTYPE_OR_NOTFOUND"
	// PriorityListGetOrCreateCommand fetches or initializes a module-backed priority list.
	PriorityListGetOrCreateCommand = "PRIORITYLIST.GETORCREATE"
	// PriorityListGetCommand fetches a module-backed priority list.
	PriorityListGetCommand = "PRIORITYLIST.GET"
	// HashSetGetOrCreateCommand fetches or initializes a module-backed hash set value.
	HashSetGetOrCreateCommand = "HASHSET.GETORCREATE"
	// HashSetLookupCommand fetches a module-backed hash set value.
	HashSetLookupCommand = "HASHSET.LOOKUP"
)

type ValkeyAdapter struct {
	client                  valkey.Client
	logger                  *zap.Logger
	valkeyAuditStreamExpiry time.Duration
}

type ValkeyAdapterOption func(*ValkeyAdapter)

func WithValkeyAuditStreamExpiry(expiry time.Duration) ValkeyAdapterOption {
	return func(va *ValkeyAdapter) {
		va.valkeyAuditStreamExpiry = expiry
	}
}

func (r *ValkeyAdapter) AuditStreamExpiry() time.Duration {
	return r.valkeyAuditStreamExpiry
}

func NewValkeyAdapter(client valkey.Client, logger *zap.Logger, opts ...ValkeyAdapterOption) *ValkeyAdapter {
	va := &ValkeyAdapter{
		client:                  client,
		logger:                  logger,
		valkeyAuditStreamExpiry: 30 * 24 * time.Hour,
	}

	for _, opt := range opts {
		opt(va)
	}

	return va
}

func (r *ValkeyAdapter) composeStorageKey(variableStorageKey string, hubName string) string {
	return variableKeyPrefix + hubName + "/" + variableStorageKey
}

func (r *ValkeyAdapter) prefixedRefs(refs []string, hubName string) []string {
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = r.composeStorageKey(ref, hubName)
	}
	return out
}

func (r *ValkeyAdapter) Exists(ctx context.Context, variableKey string, hubName string) (bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	count, err := r.client.Do(ctx, r.client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("check variable existence for %s: %w", key, err)
	}

	return count > 0, nil
}

func (r *ValkeyAdapter) GetOrCreateMetaPriorityList(ctx context.Context, variableKey string, hubName string, variableRefs []string) ([]string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	refs := r.prefixedRefs(variableRefs, hubName)
	list, err := r.client.Do(ctx, r.client.B().Arbitrary(PriorityListGetOrCreateCommand).Keys(key).Args(refs...).Build()).AsStrSlice()
	if err == nil {
		r.logger.Debug("Data received from storage", zap.String("key", key), zap.Strings("values", list))
		return list, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.logger.Info("No value found for references", zap.String("key", key))
		return nil, false, nil
	}
	return nil, false, err
}

func (r *ValkeyAdapter) GetMetaPriorityList(ctx context.Context, variableKey string, hubName string) ([]string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	list, err := r.client.Do(ctx, r.client.B().Arbitrary(PriorityListGetCommand).Keys(key).Build()).AsStrSlice()
	if err == nil {
		r.logger.Debug("Data received from storage", zap.String("key", key), zap.Strings("values", list))
		return list, true, nil
	}

	if valkey.IsValkeyNil(err) || strings.Contains(err.Error(), ValkeyWrongTypeOrNotFoundError) {
		r.logger.Info("No value found for references", zap.String("key", key))
		return nil, false, nil
	}

	return nil, false, err
}

func (r *ValkeyAdapter) GetOrCreateMetaHashSet(ctx context.Context, variableKey string, hubName string, variableStringKey string, variableSetKey string) (string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	stringKey := r.composeStorageKey(variableStringKey, hubName)
	setKey := r.composeStorageKey(variableSetKey, hubName)
	value, err := r.client.Do(ctx, r.client.B().Arbitrary(HashSetGetOrCreateCommand).Keys(key).Args(stringKey, setKey).Build()).ToString()
	if err == nil {
		r.logger.Debug("Data received from storage", zap.String("key", key), zap.String("value", value))
		return value, true, nil
	}

	if valkey.IsValkeyNil(err) {
		r.logger.Info("No value found for references", zap.String("key", key))
		return "", false, nil
	}
	return "", false, err
}

func (r *ValkeyAdapter) GetMetaHashSet(ctx context.Context, variableKey string, hubName string) (string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	value, err := r.client.Do(ctx, r.client.B().Arbitrary(HashSetLookupCommand).Keys(key).Build()).ToString()
	if err == nil {
		r.logger.Debug("Data received from storage", zap.String("key", key), zap.String("value", value))
		return value, true, nil
	}

	if valkey.IsValkeyNil(err) || strings.Contains(err.Error(), ValkeyWrongTypeOrNotFoundError) {
		r.logger.Info("No value found for references", zap.String("key", key))
		return "", false, nil
	}

	return "", false, err
}

func (r *ValkeyAdapter) GetSetAsStringSlice(ctx context.Context, variableKey string, hubName string) ([]string, error) {
	key := r.composeStorageKey(variableKey, hubName)
	valueAsSlice, err := r.client.Do(ctx, r.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		r.logger.Error("failed to get a Set value from storage", zap.Error(err), zap.String("key", key))
		return nil, err
	}

	r.logger.Debug("Data received from storage", zap.String("key", key), zap.Strings("values", valueAsSlice))
	return valueAsSlice, nil
}

// GetString retrieves the string, int and boolean variable.
// It returns the value, a boolean indicating whether the key was found, and any error encountered.
func (r *ValkeyAdapter) GetString(ctx context.Context, variableKey string, hubName string) (string, bool, error) {
	key := r.composeStorageKey(variableKey, hubName)
	value, err := r.client.Do(ctx, r.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			r.logger.Info("No value found in storage", zap.String("key", key))
			return "", false, nil
		}
		r.logger.Error("failed to get String value from storage", zap.Error(err), zap.String("key", key))
		return "", false, err
	}
	r.logger.Debug("Data received from storage", zap.String("key", key), zap.String("value", value))
	return value, true, nil
}

// GetMapAsString returns the map variable as YAML. A value is written as a YAML number only
// when it is a canonical int64 or float (see yamlValue); every other value, including "007",
// "1e3", "NaN" and integers too large for int64, is written as a string, unchanged. Numbers
// keep their value, but floats may be written in a different notation (e.g. 0.00001 as 1e-05).
func (r *ValkeyAdapter) GetMapAsString(ctx context.Context, variableKey string, hubName string) (string, error) {
	key := r.composeStorageKey(variableKey, hubName)
	raw, err := r.client.Do(ctx, r.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		r.logger.Error("failed to get Map value from storage", zap.Error(err), zap.String("key", key))
		return "", err
	}

	data := make(map[string]any, len(raw))
	for k, v := range raw {
		data[k] = yamlValue(v)
	}

	yamlData, err := yaml.Marshal(data)
	if err != nil {
		r.logger.Error("failed to marshal Map to YAML", zap.Error(err), zap.String("key", key))
		return "", err
	}

	r.logger.Debug("Data received from storage", zap.String("key", key), zap.String("yaml", string(yamlData)))
	return string(yamlData), nil
}

// yamlValue returns v as an int64 or float64 when that number formats back to exactly v, and
// v unchanged otherwise, so a number is never reinterpreted.
//   - Integer text (an optional sign and digits) becomes an int64 only in the canonical form
//     from canonicalize.go; anything else, such as "007", "-0" or a value too large for int64,
//     stays a string. It never falls back to float, which would turn an integer into a float.
//   - Other text becomes a float64 when it is finite and matches the plain decimal or the
//     shortest ('g') form. YAML always writes the 'g' form, so the notation may change but
//     the value does not.
func yamlValue(v string) any {
	if isIntegerText(v) {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil && strconv.FormatInt(i, 10) == v {
			return i
		}
		return v
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) {
		if strconv.FormatFloat(f, 'f', -1, 64) == v || strconv.FormatFloat(f, 'g', -1, 64) == v {
			return f
		}
	}
	return v
}

// isIntegerText reports whether v is an optional sign followed by one or more ASCII digits.
func isIntegerText(v string) bool {
	digits := strings.TrimLeft(v, "+-")
	if digits == "" || len(v)-len(digits) > 1 {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (r *ValkeyAdapter) GetMap(ctx context.Context, variableKey string, hubName string) (map[string]string, error) {
	key := r.composeStorageKey(variableKey, hubName)
	raw, err := r.client.Do(ctx, r.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		r.logger.Error("failed to get Map value from storage", zap.Error(err), zap.String("key", key))
		return nil, err
	}

	return raw, nil
}

func (r *ValkeyAdapter) AddElementToSet(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	r.logger.Info("Adding element to set", zap.String("variableKey", variableKey), zap.Any("value", value), zap.String("key", key))
	return r.client.B().Sadd().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) RemoveElementFromSet(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Srem().Key(key).Member(value).Build()
}

func (r *ValkeyAdapter) SetString(variableKey string, hubName string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Set().Key(key).Value(value).Build()
}

func (r *ValkeyAdapter) DeleteString(variableKey string, hubName string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Del().Key(key).Build()
}

func (r *ValkeyAdapter) IntIncrBy(variableKey string, hubName string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Incrby().Key(key).Increment(value).Build()
}

func (r *ValkeyAdapter) IntDecrBy(variableKey string, hubName string, value int64) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Decrby().Key(key).Decrement(value).Build()
}

func (r *ValkeyAdapter) SetMapEntry(variableKey string, hubName string, field string, value string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()
}

func (r *ValkeyAdapter) BulkSetMap(variableKey string, hubName string, values map[string]string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	hsetFieldValue := r.client.B().Hset().Key(key).FieldValue()
	for field, val := range values {
		hsetFieldValue = hsetFieldValue.FieldValue(field, val)
	}
	return hsetFieldValue.Build()
}

func (r *ValkeyAdapter) RemoveMapEntry(variableKey string, hubName string, field string) valkey.Completed {
	key := r.composeStorageKey(variableKey, hubName)
	return r.client.B().Hdel().Key(key).Field(field).Build()
}

func (r *ValkeyAdapter) DeleteKeysWithPrefixUsingScan(ctx context.Context, keep map[string]struct{}, hubName string) error {
	prefix := variableKeyPrefix + hubName + "/"
	keyPattern := prefix + "*"

	var cursor uint64
	for {
		scanResult, err := r.client.Do(ctx, r.client.B().Scan().Cursor(cursor).Match(keyPattern).Count(100).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("failed to scan with prefix %s: %w", prefix, err)
		}
		for _, k := range scanResult.Elements {
			res, found := strings.CutPrefix(k, prefix)
			if !found {
				return fmt.Errorf("key %s does not have expected prefix %s", k, prefix)
			}
			if _, exists := keep[res]; exists {
				continue
			}
			if _, err := r.client.Do(ctx, r.client.B().Del().Key(k).Build()).AsInt64(); err != nil {
				return fmt.Errorf("failed to delete key %s: %w", k, err)
			}
		}
		cursor = scanResult.Cursor
		if cursor == 0 {
			break
		}
	}

	return nil
}

type OperationArgs struct {
	MapKey   string
	HubName  string
	Value    string
	IntValue int64
	MapValue map[string]string
}
