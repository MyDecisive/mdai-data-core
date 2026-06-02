package variables

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeScalar_String(t *testing.T) {
	got, err := CanonicalizeScalar(json.RawMessage(`"hello"`), DataTypeString)
	require.NoError(t, err)
	assert.Equal(t, "hello", got)

	got, err = CanonicalizeScalar(json.RawMessage(`""`), DataTypeString)
	require.NoError(t, err)
	assert.Equal(t, "", got)

	_, err = CanonicalizeScalar(json.RawMessage(`123`), DataTypeString)
	require.ErrorIs(t, err, ErrInvalidDefault)
}

func TestCanonicalizeScalar_Int(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`1`, "1"},
		{`0`, "0"},
		{`-42`, "-42"},
		{`9223372036854775807`, "9223372036854775807"}, // max int64
	}
	for _, tc := range cases {
		got, err := CanonicalizeScalar(json.RawMessage(tc.raw), DataTypeInt)
		require.NoError(t, err, "input %s", tc.raw)
		assert.Equal(t, tc.want, got, "input %s", tc.raw)
	}

	for _, bad := range []string{`1.5`, `"1"`, `"not-an-int"`, `9999999999999999999`} {
		_, err := CanonicalizeScalar(json.RawMessage(bad), DataTypeInt)
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", bad)
	}
}

func TestCanonicalizeScalar_Boolean(t *testing.T) {
	got, err := CanonicalizeScalar(json.RawMessage(`true`), DataTypeBoolean)
	require.NoError(t, err)
	assert.Equal(t, "true", got)

	got, err = CanonicalizeScalar(json.RawMessage(`false`), DataTypeBoolean)
	require.NoError(t, err)
	assert.Equal(t, "false", got)

	_, err = CanonicalizeScalar(json.RawMessage(`"true"`), DataTypeBoolean)
	require.ErrorIs(t, err, ErrInvalidDefault)
}

func TestCanonicalizeScalar_Float(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`1.5`, "1.5"},
		{`1`, "1"},
		{`1.50`, "1.5"}, // trailing zero collapses
		{`-0`, "0"},     // negative zero normalizes
		{`0`, "0"},
		{`-1.5`, "-1.5"},
	}
	for _, tc := range cases {
		got, err := CanonicalizeScalar(json.RawMessage(tc.raw), DataTypeFloat)
		require.NoError(t, err, "input %s", tc.raw)
		assert.Equal(t, tc.want, got, "input %s", tc.raw)
	}

	for _, bad := range []string{`"1.5"`, `"NaN"`, `1e400`} {
		_, err := CanonicalizeScalar(json.RawMessage(bad), DataTypeFloat)
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", bad)
	}
}

func TestCanonicalizeScalar_RejectsNonScalarTypes(t *testing.T) {
	for _, dt := range []DataType{DataTypeSet, DataTypeMap, DataTypeMetaHashSet, DataTypeMetaPriorityList} {
		_, err := CanonicalizeScalar(json.RawMessage(`"x"`), dt)
		require.ErrorIs(t, err, ErrInvalidDefault, "dataType %s", dt)
	}
}

func TestCanonicalizeSet(t *testing.T) {
	got, err := CanonicalizeSet(json.RawMessage(`["a","b","c"]`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, got)

	got, err = CanonicalizeSet(json.RawMessage(`[]`))
	require.NoError(t, err)
	assert.Empty(t, got)

	for _, bad := range []string{`"a"`, `[1,2]`, `{"a":"b"}`} {
		_, err := CanonicalizeSet(json.RawMessage(bad))
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", bad)
	}
}

func TestCanonicalizeMap(t *testing.T) {
	got, err := CanonicalizeMap(json.RawMessage(`{"a":"1","b":"2"}`))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, got)

	got, err = CanonicalizeMap(json.RawMessage(`{}`))
	require.NoError(t, err)
	assert.Empty(t, got)

	for _, bad := range []string{`["a","b"]`, `{"a":1}`, `"a"`} {
		_, err := CanonicalizeMap(json.RawMessage(bad))
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", bad)
	}
}

func TestCanonicalizeSet_RejectsNullElements(t *testing.T) {
	cases := []string{
		`[null]`,
		`["a", null]`,
		`["a", null, "b"]`,
		`[ null ]`,
	}
	for _, raw := range cases {
		_, err := CanonicalizeSet(json.RawMessage(raw))
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", raw)
		require.Contains(t, err.Error(), "null")
	}
}

func TestCanonicalizeMap_RejectsNullValues(t *testing.T) {
	cases := []string{
		`{"k": null}`,
		`{"a": "1", "b": null}`,
		`{"k": null, "j": "v"}`,
	}
	for _, raw := range cases {
		_, err := CanonicalizeMap(json.RawMessage(raw))
		require.ErrorIs(t, err, ErrInvalidDefault, "input %s", raw)
		require.Contains(t, err.Error(), "null")
	}
}

func TestCanonicalizeErrorsUnwrap(t *testing.T) {
	_, err := CanonicalizeScalar(json.RawMessage(`"not-int"`), DataTypeInt)
	require.True(t, errors.Is(err, ErrInvalidDefault))
}
