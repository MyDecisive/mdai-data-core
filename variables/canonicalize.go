package ValkeyAdapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ErrInvalidDefault is the sentinel for any canonicalization failure.
var ErrInvalidDefault = errors.New("invalid default")

// CanonicalizeScalar returns the canonical Valkey string form of raw as dt.
// Non-scalar dt returns an error.
func CanonicalizeScalar(raw json.RawMessage, dt DataType) (string, error) {
	switch dt {
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
		return "", fmt.Errorf("%w: %s is not a scalar type", ErrInvalidDefault, dt)
	}
}

// CanonicalizeSet decodes raw as []string.
func CanonicalizeSet(raw json.RawMessage) ([]string, error) {
	var decoded []string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("%w: array of strings expected: %w", ErrInvalidDefault, err)
	}
	return decoded, nil
}

// CanonicalizeMap decodes raw as map[string]string.
func CanonicalizeMap(raw json.RawMessage) (map[string]string, error) {
	var decoded map[string]string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("%w: object with string values expected: %w", ErrInvalidDefault, err)
	}
	return decoded, nil
}
