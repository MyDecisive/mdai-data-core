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

// ResolveResult is the outcome of a variable read. Value is the canonical form
// (string for scalars, []string for sets, map[string]string for maps).
// IsDefault distinguishes a materialized default from a stored value.
type ResolveResult struct {
	Value     any
	Found     bool
	IsDefault bool
	DataType  DataType
}

// Resolve reads the variable; on not-found it materializes defaultRaw via the
// canonicalizers. defaultRaw==nil means no default declared. For set/map, a
// zero-length read is treated as not-found.
func Resolve(
	ctx context.Context,
	adapter Reader,
	hubName, varKey string,
	dataType DataType,
	defaultRaw json.RawMessage,
) (ResolveResult, error) {
	switch dataType {
	case DataTypeString, DataTypeInt, DataTypeFloat, DataTypeBoolean:
		stored, ok, err := adapter.GetString(ctx, varKey, hubName)
		if err != nil {
			return ResolveResult{}, err
		}
		if ok {
			return ResolveResult{Value: stored, Found: true, DataType: dataType}, nil
		}
		return resolveScalarDefault(dataType, defaultRaw)

	case DataTypeSet:
		stored, err := adapter.GetSetAsStringSlice(ctx, varKey, hubName)
		if err != nil {
			return ResolveResult{}, err
		}
		if len(stored) > 0 {
			return ResolveResult{Value: stored, Found: true, DataType: dataType}, nil
		}
		return resolveSetDefault(defaultRaw)

	case DataTypeMap:
		stored, err := adapter.GetMap(ctx, varKey, hubName)
		if err != nil {
			return ResolveResult{}, err
		}
		if len(stored) > 0 {
			return ResolveResult{Value: stored, Found: true, DataType: dataType}, nil
		}
		return resolveMapDefault(defaultRaw)

	case DataTypeMetaPriorityList:
		stored, ok, err := adapter.GetMetaPriorityList(ctx, varKey, hubName)
		if err != nil {
			return ResolveResult{}, err
		}
		if ok {
			return ResolveResult{Value: stored, Found: true, DataType: dataType}, nil
		}
		return ResolveResult{DataType: dataType}, nil

	case DataTypeMetaHashSet:
		stored, ok, err := adapter.GetMetaHashSet(ctx, varKey, hubName)
		if err != nil {
			return ResolveResult{}, err
		}
		if ok {
			return ResolveResult{Value: stored, Found: true, DataType: dataType}, nil
		}
		return ResolveResult{DataType: dataType}, nil

	default:
		return ResolveResult{}, fmt.Errorf("%w: %q", ErrUnsupportedDataType, dataType)
	}
}

func resolveScalarDefault(dataType DataType, defaultRaw json.RawMessage) (ResolveResult, error) {
	if defaultRaw == nil {
		return ResolveResult{DataType: dataType}, nil
	}
	canonical, err := CanonicalizeScalar(defaultRaw, dataType)
	if err != nil {
		return ResolveResult{}, err
	}
	return ResolveResult{Value: canonical, Found: true, IsDefault: true, DataType: dataType}, nil
}

func resolveSetDefault(defaultRaw json.RawMessage) (ResolveResult, error) {
	if defaultRaw == nil {
		return ResolveResult{DataType: DataTypeSet}, nil
	}
	canonical, err := CanonicalizeSet(defaultRaw)
	if err != nil {
		return ResolveResult{}, err
	}
	return ResolveResult{Value: canonical, Found: true, IsDefault: true, DataType: DataTypeSet}, nil
}

func resolveMapDefault(defaultRaw json.RawMessage) (ResolveResult, error) {
	if defaultRaw == nil {
		return ResolveResult{DataType: DataTypeMap}, nil
	}
	canonical, err := CanonicalizeMap(defaultRaw)
	if err != nil {
		return ResolveResult{}, err
	}
	return ResolveResult{Value: canonical, Found: true, IsDefault: true, DataType: DataTypeMap}, nil
}

// Typed returns Value as int64, float64, bool, string, []string, or
// map[string]string per the originating dataType.
func (r ResolveResult) Typed() (any, error) {
	switch r.DataType {
	case DataTypeString, DataTypeMetaHashSet:
		str, ok := r.Value.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected string for %s, got %T", r.DataType, r.Value)
		}
		return str, nil

	case DataTypeInt:
		str, ok := r.Value.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for int, got %T", r.Value)
		}
		return strconv.ParseInt(str, 10, 64)

	case DataTypeFloat:
		str, ok := r.Value.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for float, got %T", r.Value)
		}
		return strconv.ParseFloat(str, 64)

	case DataTypeBoolean:
		str, ok := r.Value.(string)
		if !ok {
			return nil, fmt.Errorf("typed: expected canonical string for boolean, got %T", r.Value)
		}
		return strconv.ParseBool(str)

	case DataTypeSet, DataTypeMetaPriorityList:
		slice, ok := r.Value.([]string)
		if !ok {
			return nil, fmt.Errorf("typed: expected []string for %s, got %T", r.DataType, r.Value)
		}
		return slice, nil

	case DataTypeMap:
		hash, ok := r.Value.(map[string]string)
		if !ok {
			return nil, fmt.Errorf("typed: expected map[string]string for map, got %T", r.Value)
		}
		return hash, nil

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDataType, r.DataType)
	}
}
