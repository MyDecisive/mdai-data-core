package variables

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

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, "42", res.Value)
}

func TestResolve_ScalarDefaultApplied(t *testing.T) {
	reader := newFakeReader()
	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.True(t, res.IsDefault)
	assert.Equal(t, "100", res.Value)
}

func TestResolve_ScalarNoStoredNoDefault(t *testing.T) {
	reader := newFakeReader()
	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeString, nil)
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Nil(t, res.Value)
}

func TestResolve_ScalarStoredEmptyIsNotDefault(t *testing.T) {
	reader := newFakeReader()
	reader.str["h:v"] = ""
	reader.strOK["h:v"] = true

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeString, json.RawMessage(`"fallback"`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, "", res.Value)
}

func TestResolve_FloatDefaultCanonicalized(t *testing.T) {
	reader := newFakeReader()
	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeFloat, json.RawMessage(`1.50`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.True(t, res.IsDefault)
	assert.Equal(t, "1.5", res.Value)
}

func TestResolve_SetStored(t *testing.T) {
	reader := newFakeReader()
	reader.set["h:v"] = []string{"a", "b"}

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, json.RawMessage(`["x"]`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, []string{"a", "b"}, res.Value)
}

func TestResolve_SetEmptyTreatedAsAbsent(t *testing.T) {
	reader := newFakeReader()
	reader.set["h:v"] = []string{}

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, json.RawMessage(`["x"]`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.True(t, res.IsDefault)
	assert.Equal(t, []string{"x"}, res.Value)
}

func TestResolve_SetNoDefault(t *testing.T) {
	reader := newFakeReader()
	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, nil)
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Nil(t, res.Value)
}

func TestResolve_MapStored(t *testing.T) {
	reader := newFakeReader()
	reader.mp["h:v"] = map[string]string{"a": "1"}

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeMap, json.RawMessage(`{"x":"y"}`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, map[string]string{"a": "1"}, res.Value)
}

func TestResolve_MapEmptyTreatedAsAbsent(t *testing.T) {
	reader := newFakeReader()
	reader.mp["h:v"] = map[string]string{}

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeMap, json.RawMessage(`{"x":"y"}`))
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.True(t, res.IsDefault)
	assert.Equal(t, map[string]string{"x": "y"}, res.Value)
}

func TestResolve_MetaTypesNoDefault(t *testing.T) {
	reader := newFakeReader()
	reader.metaPL["h:p"] = []string{"a"}
	reader.metaPLOK["h:p"] = true
	reader.metaHS["h:h"] = "value"
	reader.metaHSOK["h:h"] = true

	res, err := Resolve(context.Background(), reader, "h", "p", DataTypeMetaPriorityList, nil)
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, []string{"a"}, res.Value)

	res, err = Resolve(context.Background(), reader, "h", "h", DataTypeMetaHashSet, nil)
	require.NoError(t, err)
	assert.True(t, res.Found)
	assert.False(t, res.IsDefault)
	assert.Equal(t, "value", res.Value)
}

func TestResolve_MetaTypesAbsent(t *testing.T) {
	reader := newFakeReader()
	res, err := Resolve(context.Background(), reader, "h", "p", DataTypeMetaPriorityList, nil)
	require.NoError(t, err)
	assert.False(t, res.Found)
	assert.Nil(t, res.Value)
}

func TestResolve_ErrorPropagation(t *testing.T) {
	reader := newFakeReader()
	reader.forceErr = errors.New("boom")

	_, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`100`))
	require.Error(t, err)
}

func TestResolve_UnsupportedDataTypeSentinel(t *testing.T) {
	reader := newFakeReader()
	_, err := Resolve(context.Background(), reader, "h", "v", DataType("bogus"), nil)
	require.ErrorIs(t, err, ErrUnsupportedDataType)
}

func TestResolveResult_TypedUnsupportedDataTypeSentinel(t *testing.T) {
	_, err := ResolveResult{Value: "x", DataType: DataType("bogus")}.Typed()
	require.ErrorIs(t, err, ErrUnsupportedDataType)
}

func TestResolve_InvalidDefaultSurfaces(t *testing.T) {
	reader := newFakeReader()
	_, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, json.RawMessage(`"not-an-int"`))
	require.ErrorIs(t, err, ErrInvalidDefault)
}

func TestResolveResult_Typed_Scalars(t *testing.T) {
	value, err := ResolveResult{Value: "42", DataType: DataTypeInt}.Typed()
	require.NoError(t, err)
	assert.Equal(t, int64(42), value)

	value, err = ResolveResult{Value: "1.5", DataType: DataTypeFloat}.Typed()
	require.NoError(t, err)
	assert.Equal(t, 1.5, value)

	value, err = ResolveResult{Value: "true", DataType: DataTypeBoolean}.Typed()
	require.NoError(t, err)
	assert.Equal(t, true, value)

	value, err = ResolveResult{Value: "hello", DataType: DataTypeString}.Typed()
	require.NoError(t, err)
	assert.Equal(t, "hello", value)
}

func TestResolveResult_Typed_Collections(t *testing.T) {
	value, err := ResolveResult{Value: []string{"a", "b"}, DataType: DataTypeSet}.Typed()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, value)

	value, err = ResolveResult{Value: map[string]string{"a": "1"}, DataType: DataTypeMap}.Typed()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, value)
}

func TestResolveResult_Typed_MetaTypes(t *testing.T) {
	value, err := ResolveResult{Value: []string{"a"}, DataType: DataTypeMetaPriorityList}.Typed()
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, value)

	value, err = ResolveResult{Value: "value", DataType: DataTypeMetaHashSet}.Typed()
	require.NoError(t, err)
	assert.Equal(t, "value", value)
}

func TestResolveResult_Typed_TypeMismatch(t *testing.T) {
	_, err := ResolveResult{Value: "not-int", DataType: DataTypeInt}.Typed()
	require.Error(t, err)

	_, err = ResolveResult{Value: 42, DataType: DataTypeString}.Typed()
	require.Error(t, err)
}

func TestResolve_TypedIntegratesWithResolve(t *testing.T) {
	reader := newFakeReader()
	reader.str["h:v"] = "42"
	reader.strOK["h:v"] = true

	res, err := Resolve(context.Background(), reader, "h", "v", DataTypeInt, nil)
	require.NoError(t, err)
	typed, err := res.Typed()
	require.NoError(t, err)
	assert.Equal(t, int64(42), typed)
}

func TestValkeyAdapterSatisfiesReader(t *testing.T) {
	var _ Reader = (*ValkeyAdapter)(nil)
}

func TestCanonicalizeSet_RejectsNullElementsViaResolve(t *testing.T) {
	reader := newFakeReader()
	_, err := Resolve(context.Background(), reader, "h", "v", DataTypeSet, json.RawMessage(`["a", null]`))
	require.ErrorIs(t, err, ErrInvalidDefault)
}
