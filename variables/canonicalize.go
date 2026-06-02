package variables

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ErrInvalidDefault is the sentinel for any canonicalization failure.
var ErrInvalidDefault = errors.New("invalid default")

var nullLiteral = []byte("null")

func isNullLiteral(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), nullLiteral)
}

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

// CanonicalizeSet decodes raw as []string.
func CanonicalizeSet(raw json.RawMessage) ([]string, error) {
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return nil, fmt.Errorf("%w: array of strings expected: %w", ErrInvalidDefault, err)
	}
	out := make([]string, 0, len(elements))
	for i, elem := range elements {
		if isNullLiteral(elem) {
			return nil, fmt.Errorf("%w: array element %d is null", ErrInvalidDefault, i)
		}
		var member string
		if err := json.Unmarshal(elem, &member); err != nil {
			return nil, fmt.Errorf("%w: array of strings expected: %w", ErrInvalidDefault, err)
		}
		out = append(out, member)
	}
	return out, nil
}

// CanonicalizeMap decodes raw as map[string]string.
func CanonicalizeMap(raw json.RawMessage) (map[string]string, error) {
	var entries map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("%w: object with string values expected: %w", ErrInvalidDefault, err)
	}
	out := make(map[string]string, len(entries))
	for key, rawValue := range entries {
		if isNullLiteral(rawValue) {
			return nil, fmt.Errorf("%w: value for key %q is null", ErrInvalidDefault, key)
		}
		var value string
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return nil, fmt.Errorf("%w: object with string values expected: %w", ErrInvalidDefault, err)
		}
		out[key] = value
	}
	return out, nil
}
