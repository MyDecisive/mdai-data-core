package variables

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// Reader is the read interface Resolve depends on; ValkeyAdapter satisfies it.
type Reader interface {
	GetString(ctx context.Context, variableKey, hubName string) (string, bool, error)
	GetSetAsStringSlice(ctx context.Context, variableKey, hubName string) ([]string, error)
	GetMap(ctx context.Context, variableKey, hubName string) (map[string]string, error)
	GetMetaPriorityList(ctx context.Context, variableKey, hubName string) ([]string, bool, error)
	GetMetaHashSet(ctx context.Context, variableKey, hubName string) (string, bool, error)
}

// Resolve reads the variable; on not-found it materializes defaultRaw via the
// canonicalizers and returns isDefault=true. defaultRaw==nil means no default.
// For set/map, a zero-length read is treated as not-found (Valkey collapses
// empty collections to absent).
func Resolve(
	ctx context.Context,
	adapter Reader,
	hubName, varKey string,
	dt DataType,
	defaultRaw json.RawMessage,
) (value any, found bool, isDefault bool, err error) {
	switch dt {
	case DataTypeString, DataTypeInt, DataTypeFloat, DataTypeBoolean:
		stored, ok, err := adapter.GetString(ctx, varKey, hubName)
		if err != nil {
			return nil, false, false, err
		}
		if ok {
			return stored, true, false, nil
		}
		return resolveScalarDefault(dt, defaultRaw)

	case DataTypeSet:
		stored, err := adapter.GetSetAsStringSlice(ctx, varKey, hubName)
		if err != nil {
			return nil, false, false, err
		}
		if len(stored) > 0 {
			return stored, true, false, nil
		}
		return resolveSetDefault(defaultRaw)

	case DataTypeMap:
		stored, err := adapter.GetMap(ctx, varKey, hubName)
		if err != nil {
			return nil, false, false, err
		}
		if len(stored) > 0 {
			return stored, true, false, nil
		}
		return resolveMapDefault(defaultRaw)

	case DataTypeMetaPriorityList:
		stored, ok, err := adapter.GetMetaPriorityList(ctx, varKey, hubName)
		if err != nil {
			return nil, false, false, err
		}
		if ok {
			return stored, true, false, nil
		}
		return nil, false, false, nil

	case DataTypeMetaHashSet:
		stored, ok, err := adapter.GetMetaHashSet(ctx, varKey, hubName)
		if err != nil {
			return nil, false, false, err
		}
		if ok {
			return stored, true, false, nil
		}
		return nil, false, false, nil

	default:
		return nil, false, false, fmt.Errorf("%w: %q", ErrUnsupportedDataType, dt)
	}
}

func resolveScalarDefault(dt DataType, defaultRaw json.RawMessage) (value any, found bool, isDefault bool, err error) {
	if defaultRaw == nil {
		return nil, false, false, nil
	}
	canonical, err := CanonicalizeScalar(defaultRaw, dt)
	if err != nil {
		return nil, false, false, err
	}
	return canonical, true, true, nil
}

func resolveSetDefault(defaultRaw json.RawMessage) (value any, found bool, isDefault bool, err error) {
	if defaultRaw == nil {
		return nil, false, false, nil
	}
	canonical, err := CanonicalizeSet(defaultRaw)
	if err != nil {
		return nil, false, false, err
	}
	return canonical, true, true, nil
}

func resolveMapDefault(defaultRaw json.RawMessage) (value any, found bool, isDefault bool, err error) {
	if defaultRaw == nil {
		return nil, false, false, nil
	}
	canonical, err := CanonicalizeMap(defaultRaw)
	if err != nil {
		return nil, false, false, err
	}
	return canonical, true, true, nil
}

// Typed converts a canonical value (from Resolve) into the typed Go shape for dt:
// int64, float64, bool, string, []string, or map[string]string.
func Typed(canonical any, dt DataType) (any, error) {
	switch dt {
	case DataTypeString, DataTypeMetaHashSet:
		str, ok := canonical.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected string for %s, got %T", dt, canonical)
		}
		return str, nil

	case DataTypeInt:
		str, ok := canonical.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for int, got %T", canonical)
		}
		return strconv.ParseInt(str, 10, 64)

	case DataTypeFloat:
		str, ok := canonical.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for float, got %T", canonical)
		}
		return strconv.ParseFloat(str, 64)

	case DataTypeBoolean:
		str, ok := canonical.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for boolean, got %T", canonical)
		}
		return strconv.ParseBool(str)

	case DataTypeSet, DataTypeMetaPriorityList:
		slice, ok := canonical.([]string)
		if !ok {
			return nil, fmt.Errorf("typed: expected []string for %s, got %T", dt, canonical)
		}
		return slice, nil

	case DataTypeMap:
		hash, ok := canonical.(map[string]string)
		if !ok {
			return nil, fmt.Errorf("typed: expected map[string]string for map, got %T", canonical)
		}
		return hash, nil

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDataType, dt)
	}
}
