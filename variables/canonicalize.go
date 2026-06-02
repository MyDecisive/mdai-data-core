package variables

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ErrInvalidDefault is the sentinel for any canonicalization failure.
var ErrInvalidDefault = errors.New("invalid default")

// CanonicalizeScalar returns the canonical Valkey string form of raw as dataType.
// Non-scalar dataType returns an error.
func CanonicalizeScalar(raw json.RawMessage, dataType DataType) (string, error) {
	switch dataType {
	case DataTypeString:
		var decoded string
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return "", fmt.Errorf("%w: string expected: %w", ErrInvalidDefault, err)
		}
		return decoded, nil

	case DataTypeInt:
		var decoded int64
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return "", fmt.Errorf("%w: int expected: %w", ErrInvalidDefault, err)
		}
		return strconv.FormatInt(decoded, 10), nil

	case DataTypeBoolean:
		var decoded bool
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return "", fmt.Errorf("%w: boolean expected: %w", ErrInvalidDefault, err)
		}
		return strconv.FormatBool(decoded), nil

	case DataTypeFloat:
		var decoded float64
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return "", fmt.Errorf("%w: float expected: %w", ErrInvalidDefault, err)
		}
		if math.IsNaN(decoded) || math.IsInf(decoded, 0) {
			return "", fmt.Errorf("%w: NaN and ±Inf are not allowed for float", ErrInvalidDefault)
		}
		if decoded == 0 {
			decoded = 0 // normalize -0 → 0
		}
		return strconv.FormatFloat(decoded, 'g', -1, 64), nil

	default:
		return "", fmt.Errorf("%w: %s is not a scalar type", ErrInvalidDefault, dataType)
	}
}

// CanonicalizeSet decodes raw as []string. Null elements and non-string
// elements are rejected.
func CanonicalizeSet(raw json.RawMessage) ([]string, error) {
	var elements []any
	if err := json.Unmarshal(raw, &elements); err != nil {
		return nil, fmt.Errorf("%w: array of strings expected: %w", ErrInvalidDefault, err)
	}
	out := make([]string, 0, len(elements))
	for i, elem := range elements {
		if elem == nil {
			return nil, fmt.Errorf("%w: array element %d is null", ErrInvalidDefault, i)
		}
		s, ok := elem.(string)
		if !ok {
			return nil, fmt.Errorf("%w: array element %d is not a string (got %T)", ErrInvalidDefault, i, elem)
		}
		out = append(out, s)
	}
	return out, nil
}

// CanonicalizeMap decodes raw as map[string]string. Null values and non-string
// values are rejected.
func CanonicalizeMap(raw json.RawMessage) (map[string]string, error) {
	var entries map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("%w: object with string values expected: %w", ErrInvalidDefault, err)
	}
	out := make(map[string]string, len(entries))
	for key, value := range entries {
		if value == nil {
			return nil, fmt.Errorf("%w: value for key %q is null", ErrInvalidDefault, key)
		}
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: value for key %q is not a string (got %T)", ErrInvalidDefault, key, value)
		}
		out[key] = s
	}
	return out, nil
}
