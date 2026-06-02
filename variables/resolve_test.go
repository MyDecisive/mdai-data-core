package ValkeyAdapter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeReader struct {
	str   map[string]string
	strOK map[string]bool

	set map[string][]string
	mp  map[string]map[string]string

	metaPL    map[string][]string
	metaPLOK  map[string]bool
	metaHS    map[string]string
	metaHSOK  map[string]bool
	forceErr  error
	hubPrefix string
}

func newFakeReader() *fakeReader {
	return &fakeReader{
		str:      map[string]string{},
		strOK:    map[string]bool{},
		set:      map[string][]string{},
		mp:       map[string]map[string]string{},
		metaPL:   map[string][]string{},
		metaPLOK: map[string]bool{},
		metaHS:   map[string]string{},
		metaHSOK: map[string]bool{},
	}
}

func (f *fakeReader) key(varKey, hub string) string {
	if f.hubPrefix != "" {
		return f.hubPrefix + ":" + varKey
	}
	return hub + ":" + varKey
}

func (f *fakeReader) GetString(_ context.Context, varKey, hub string) (string, bool, error) {
	if f.forceErr != nil {
		return "", false, f.forceErr
	}
	storageKey := f.key(varKey, hub)
	return f.str[storageKey], f.strOK[storageKey], nil
}

func (f *fakeReader) GetSetAsStringSlice(_ context.Context, varKey, hub string) ([]string, error) {
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	return f.set[f.key(varKey, hub)], nil
}

func (f *fakeReader) GetMap(_ context.Context, varKey, hub string) (map[string]string, error) {
	if f.forceErr != nil {
		return nil, f.forceErr
	}
	return f.mp[f.key(varKey, hub)], nil
}

func (f *fakeReader) GetMetaPriorityList(_ context.Context, varKey, hub string) ([]string, bool, error) {
	if f.forceErr != nil {
		return nil, false, f.forceErr
	}
	storageKey := f.key(varKey, hub)
	return f.metaPL[storageKey], f.metaPLOK[storageKey], nil
}

func (f *fakeReader) GetMetaHashSet(_ context.Context, varKey, hub string) (string, bool, error) {
	if f.forceErr != nil {
		return "", false, f.forceErr
	}
	storageKey := f.key(varKey, hub)
	return f.metaHS[storageKey], f.metaHSOK[storageKey], nil
}

func TestResolve_ScalarStored(t *testing.T) {
	reader := newFakeReader()
	reader.str["h:v"] = "42"
	reader.strOK["h:v"] = true

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, "42", val)
}

func TestResolve_ScalarDefaultApplied(t *testing.T) {
	reader := newFakeReader()
	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, isDefault)
	assert.Equal(t, "100", val)
}

func TestResolve_ScalarNoStoredNoDefault(t *testing.T) {
	reader := newFakeReader()
	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeString, nil)
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, isDefault)
	assert.Nil(t, val)
}

func TestResolve_ScalarStoredEmptyIsNotDefault(t *testing.T) {
	reader := newFakeReader()
	reader.str["h:v"] = ""
	reader.strOK["h:v"] = true

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeString, json.RawMessage(`"fallback"`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, "", val)
}

func TestResolve_FloatDefaultCanonicalized(t *testing.T) {
	reader := newFakeReader()
	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeFloat, json.RawMessage(`1.50`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, isDefault)
	assert.Equal(t, "1.5", val)
}

func TestResolve_SetStored(t *testing.T) {
	reader := newFakeReader()
	reader.set["h:v"] = []string{"a", "b"}

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, json.RawMessage(`["x"]`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, []string{"a", "b"}, val)
}

func TestResolve_SetEmptyTreatedAsAbsent(t *testing.T) {
	reader := newFakeReader()
	reader.set["h:v"] = []string{}

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, json.RawMessage(`["x"]`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, isDefault)
	assert.Equal(t, []string{"x"}, val)
}

func TestResolve_SetNoDefault(t *testing.T) {
	reader := newFakeReader()
	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, nil)
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, isDefault)
	assert.Nil(t, val)
}

func TestResolve_MapStored(t *testing.T) {
	reader := newFakeReader()
	reader.mp["h:v"] = map[string]string{"a": "1"}

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeMap, json.RawMessage(`{"x":"y"}`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, map[string]string{"a": "1"}, val)
}

func TestResolve_MapEmptyTreatedAsAbsent(t *testing.T) {
	reader := newFakeReader()
	reader.mp["h:v"] = map[string]string{}

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "v", DataTypeMap, json.RawMessage(`{"x":"y"}`))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, isDefault)
	assert.Equal(t, map[string]string{"x": "y"}, val)
}

func TestResolve_MetaTypesNoDefault(t *testing.T) {
	reader := newFakeReader()
	reader.metaPL["h:p"] = []string{"a"}
	reader.metaPLOK["h:p"] = true
	reader.metaHS["h:h"] = "value"
	reader.metaHSOK["h:h"] = true

	val, found, isDefault, err := Resolve(context.Background(), reader, "h", "p", DataTypeMetaPriorityList, nil)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, []string{"a"}, val)

	val, found, isDefault, err = Resolve(context.Background(), reader, "h", "h", DataTypeMetaHashSet, nil)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, isDefault)
	assert.Equal(t, "value", val)
}

func TestResolve_MetaTypesAbsent(t *testing.T) {
	reader := newFakeReader()
	val, found, _, err := Resolve(context.Background(), reader, "h", "p", DataTypeMetaPriorityList, nil)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestResolve_ErrorPropagation(t *testing.T) {
	reader := newFakeReader()
	reader.forceErr = errors.New("boom")

	_, _, _, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.Error(t, err)
}

func TestResolve_UnsupportedDataTypeSentinel(t *testing.T) {
	reader := newFakeReader()
	_, _, _, err := Resolve(context.Background(), reader, "h", "v", DataType("bogus"), nil)
	require.ErrorIs(t, err, ErrUnsupportedDataType)
}

func TestTyped_UnsupportedDataTypeSentinel(t *testing.T) {
	_, err := Typed("x", DataType("bogus"))
	require.ErrorIs(t, err, ErrUnsupportedDataType)
}

func TestResolve_InvalidDefaultSurfaces(t *testing.T) {
	reader := newFakeReader()
	_, _, _, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`"not-an-int"`))
	require.ErrorIs(t, err, ErrInvalidDefault)
}

func TestTyped_Scalars(t *testing.T) {
	value, err := Typed("42", DataTypeInt)
	require.NoError(t, err)
	assert.Equal(t, int64(42), value)

	value, err = Typed("1.5", DataTypeFloat)
	require.NoError(t, err)
	assert.Equal(t, 1.5, value)

	value, err = Typed("true", DataTypeBoolean)
	require.NoError(t, err)
	assert.Equal(t, true, value)

	value, err = Typed("hello", DataTypeString)
	require.NoError(t, err)
	assert.Equal(t, "hello", value)
}

func TestTyped_Collections(t *testing.T) {
	value, err := Typed([]string{"a", "b"}, DataTypeSet)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, value)

	value, err = Typed(map[string]string{"a": "1"}, DataTypeMap)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, value)
}

func TestTyped_MetaTypes(t *testing.T) {
	value, err := Typed([]string{"a"}, DataTypeMetaPriorityList)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, value)

	value, err = Typed("value", DataTypeMetaHashSet)
	require.NoError(t, err)
	assert.Equal(t, "value", value)
}

func TestTyped_TypeMismatch(t *testing.T) {
	_, err := Typed("not-int", DataTypeInt)
	require.Error(t, err)

	_, err = Typed(42, DataTypeString)
	require.Error(t, err)
}

func TestValkeyAdapterSatisfiesReader(t *testing.T) {
	var _ Reader = (*ValkeyAdapter)(nil)
}
