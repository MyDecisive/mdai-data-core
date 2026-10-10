package variables

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
	vmock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func newAdapterWithMock(t *testing.T) (*ValkeyAdapter, *vmock.Client, context.Context, *gomock.Controller) {
	logger := zap.NewNop()
	t.Helper()
	ctrl := gomock.NewController(t)
	client := vmock.NewClient(ctrl)
	adapter := NewValkeyAdapter(client, logger)
	return adapter, client, context.Background(), ctrl
}

func TestComposeStorageKey(t *testing.T) {
	logger := zap.NewNop()
	adapter := NewValkeyAdapter(nil, logger)
	assert.Equal(t, "variable/hub-1/myvar", adapter.composeStorageKey("myvar", "hub-1"))
}

func TestGetString(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/foo"

	client.EXPECT().
		Do(ctx, vmock.Match("GET", key)).
		Return(vmock.Result(vmock.ValkeyString("bar")))
	val, found, err := adapter.GetString(ctx, "foo", "hub")

	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "bar", val)

	client.EXPECT().
		Do(ctx, vmock.Match("GET", key)).
		Return(vmock.Result(vmock.ValkeyNil()))
	_, found, err = adapter.GetString(ctx, "foo", "hub")

	require.NoError(t, err)
	assert.False(t, found)
}

func TestExists(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/foo"

	client.EXPECT().
		Do(ctx, vmock.Match("EXISTS", key)).
		Return(vmock.Result(vmock.ValkeyInt64(1)))
	exists, err := adapter.Exists(ctx, "foo", "hub")

	require.NoError(t, err)
	assert.True(t, exists)

	client.EXPECT().
		Do(ctx, vmock.Match("EXISTS", key)).
		Return(vmock.Result(vmock.ValkeyInt64(0)))
	exists, err = adapter.Exists(ctx, "foo", "hub")

	require.NoError(t, err)
	assert.False(t, exists)

	client.EXPECT().
		Do(ctx, vmock.Match("EXISTS", key)).
		Return(vmock.Result(vmock.ValkeyError("boom")))
	exists, err = adapter.Exists(ctx, "foo", "hub")

	require.Error(t, err)
	assert.False(t, exists)
	assert.ErrorContains(t, err, "check variable existence for "+key)
	assert.ErrorContains(t, err, "boom")
}

func TestGetSetAsStringSlice(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/myset"

	client.EXPECT().
		Do(ctx, vmock.Match("SMEMBERS", key)).
		Return(vmock.Result(
			vmock.ValkeyArray(vmock.ValkeyBlobString("first"), vmock.ValkeyBlobString("second")),
		))

	got, err := adapter.GetSetAsStringSlice(ctx, "myset", "hub")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"first", "second"}, got)
}

func TestGetMapAsString_YAMLConversion(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/myhash"

	client.EXPECT().
		Do(ctx, vmock.Match("HGETALL", key)).
		Return(vmock.Result(vmock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"a": vmock.ValkeyBlobString("1"),
			"b": vmock.ValkeyBlobString("two"),
			"c": vmock.ValkeyBlobString("3.14"),
		})))

	out, err := adapter.GetMapAsString(ctx, "myhash", "hub")
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &got))

	expected := map[string]any{
		"a": 1,     // int
		"b": "two", // string
		"c": 3.14,  // float64
	}

	assert.Equal(t, expected, got)
}

func TestGetMap(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	key := "variable/hub/myhash"

	client.EXPECT().
		Do(ctx, vmock.Match("HGETALL", key)).
		Return(vmock.Result(vmock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"a": vmock.ValkeyBlobString("1"),
			"b": vmock.ValkeyBlobString("two"),
			"c": vmock.ValkeyBlobString("3.14"),
		})))

	got, err := adapter.GetMap(ctx, "myhash", "hub")
	assert.NoError(t, err)

	expected := map[string]string{
		"a": "1",    // int
		"b": "two",  // string
		"c": "3.14", // float64
	}

	assert.Equal(t, expected, got)
}

func TestDeleteKeysWithPrefixUsingScan(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	const prefix = "variable/hub/"
	scanPattern := prefix + "*"
	keyToDelete := prefix + "killme"
	keyToKeep := prefix + "keepme"

	scanReply := vmock.ValkeyArray(
		vmock.ValkeyBlobString("0"),
		vmock.ValkeyArray(
			vmock.ValkeyBlobString(keyToDelete),
			vmock.ValkeyBlobString(keyToKeep),
		),
	)

	gomock.InOrder(
		client.EXPECT().
			Do(ctx, vmock.Match(
				"SCAN", "0",
				"MATCH", scanPattern,
				"COUNT", "100",
			)).
			Return(vmock.Result(scanReply)),

		client.EXPECT().
			Do(ctx, vmock.Match("DEL", keyToDelete)).
			Return(vmock.Result(vmock.ValkeyInt64(1))),
	)

	keep := map[string]struct{}{"keepme": {}}
	err := adapter.DeleteKeysWithPrefixUsingScan(ctx, keep, "hub")
	require.NoError(t, err)
}

func TestGetOrCreateMetaPriorityList(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	varKey := "parent"
	refs := []string{"ref1", "ref2"}
	hubKey := "variable/hub/"
	key := hubKey + varKey
	r1 := hubKey + refs[0]
	r2 := hubKey + refs[1]

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(PriorityListGetOrCreateCommand, key, r1, r2),
		).
		Return(vmock.Result(
			vmock.ValkeyArray(
				vmock.ValkeyBlobString(r1),
				vmock.ValkeyBlobString(r2),
			),
		))

	list, found, err := adapter.GetOrCreateMetaPriorityList(ctx, varKey, "hub", refs)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []string{r1, r2}, list)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(PriorityListGetOrCreateCommand, key, r1, r2),
		).
		Return(vmock.Result(vmock.ValkeyNil()))

	list, found, err = adapter.GetOrCreateMetaPriorityList(ctx, varKey, "hub", refs)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, list)
}

func TestGetMetaPriorityList(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	varKey := "parent"
	key := "variable/hub/" + varKey
	const (
		r1 = "variable/hub/ref1"
		r2 = "variable/hub/ref2"
	)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(PriorityListGetCommand, key),
		).
		Return(vmock.Result(
			vmock.ValkeyArray(
				vmock.ValkeyBlobString(r1),
				vmock.ValkeyBlobString(r2),
			),
		))

	list, found, err := adapter.GetMetaPriorityList(ctx, varKey, "hub")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []string{r1, r2}, list)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(PriorityListGetCommand, key),
		).
		Return(vmock.Result(vmock.ValkeyError(ValkeyWrongTypeOrNotFoundError)))

	list, found, err = adapter.GetMetaPriorityList(ctx, varKey, "hub")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, list)
}

func TestGetOrCreateMetaHashSet(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	varKey := "color"
	strKeyInput := "strKey"
	setKeyInput := "setKey"
	hubKey := "variable/hub/"
	key := hubKey + varKey
	strKey := hubKey + strKeyInput
	setKey := hubKey + setKeyInput
	wantVal := "blue"

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(HashSetGetOrCreateCommand, key, strKey, setKey),
		).
		Return(vmock.Result(vmock.ValkeyBlobString(wantVal)))

	got, found, err := adapter.GetOrCreateMetaHashSet(ctx, varKey, "hub", strKeyInput, setKeyInput)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, wantVal, got)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(HashSetGetOrCreateCommand, key, strKey, setKey),
		).
		Return(vmock.Result(vmock.ValkeyNil()))

	got, found, err = adapter.GetOrCreateMetaHashSet(ctx, varKey, "hub", strKeyInput, setKeyInput)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, got)
}

func TestGetMetaHashSet(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	varKey := "color"
	key := "variable/hub/" + varKey
	wantVal := "blue"

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(HashSetLookupCommand, key),
		).
		Return(vmock.Result(vmock.ValkeyBlobString(wantVal)))

	got, found, err := adapter.GetMetaHashSet(ctx, varKey, "hub")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, wantVal, got)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match(HashSetLookupCommand, key),
		).
		Return(vmock.Result(vmock.ValkeyError(ValkeyWrongTypeOrNotFoundError)))

	got, found, err = adapter.GetMetaHashSet(ctx, varKey, "hub")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, got)
}

func TestWithValkeyAuditStreamExpiryOption(t *testing.T) {
	logger := zap.NewNop()

	defaultTTL := 30 * 24 * time.Hour

	a1 := NewValkeyAdapter(nil, logger)
	assert.Equal(t, defaultTTL, a1.valkeyAuditStreamExpiry)

	customTTL := 12 * time.Hour
	a2 := NewValkeyAdapter(nil, logger,
		WithValkeyAuditStreamExpiry(customTTL),
	)
	assert.Equal(t, customTTL, a2.valkeyAuditStreamExpiry)
}

func TestYAMLValue(t *testing.T) {
	numbers := map[string]any{
		"1":                    int64(1),
		"-42":                  int64(-42),
		"0":                    int64(0),
		"9223372036854775807":  int64(9223372036854775807),
		"-9223372036854775808": int64(-9223372036854775808),
		"3.14":                 3.14,
		"-0.5":                 -0.5,
		"1500000.5":            1500000.5,
		"0.00001":              0.00001,
		"1e-05":                0.00001,
	}
	for in, want := range numbers {
		assert.Equal(t, want, yamlValue(in), "yamlValue(%q)", in)
	}

	// Anything that would not format back to the same text stays a string, unchanged.
	unchanged := []string{
		"", "two", "007", "+1", "1.50", ".5", "5.", "1e3", "1E-05",
		"NaN", "nan", "Inf", "+Inf", "-Inf", "Infinity", "inf",
		"0x1F", "0x1p-2", "1_000",
		"12345678901234567890", // too large for int64
		" 1", "1 ",
		// Integer text that doesn't fit in int64 must not fall back to float, even when the
		// float formats back to the same digits.
		"100000000000000000000", "-100000000000000000000", "9223372036854775808",
		"-0", "+0", "--1", "+-1",
	}
	for _, in := range unchanged {
		assert.Equal(t, in, yamlValue(in), "yamlValue(%q)", in)
	}
}

func TestGetMapAsString_KeepsNonCanonicalNumbersAsStrings(t *testing.T) {
	adapter, client, ctx, ctrl := newAdapterWithMock(t)
	defer ctrl.Finish()

	const key = "variable/hub/myhash"
	values := map[string]string{
		"zip":   "007",
		"ratio": "NaN",
		"count": "1e3",
		"big":   "12345678901234567890",
		"huge":  "100000000000000000000", // fits a float exactly, but must not become 1e+20
		"port":  "8080",
	}

	stored := map[string]valkey.ValkeyMessage{}
	for k, v := range values {
		stored[k] = vmock.ValkeyBlobString(v)
	}
	client.EXPECT().
		Do(ctx, vmock.Match("HGETALL", key)).
		Return(vmock.Result(vmock.ValkeyMap(stored)))

	out, err := adapter.GetMapAsString(ctx, "myhash", "hub")
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &got))
	assert.Equal(t, map[string]any{
		"zip":   "007",
		"ratio": "NaN",
		"count": "1e3",
		"big":   "12345678901234567890",
		"huge":  "100000000000000000000",
		"port":  8080,
	}, got, "YAML output:\n%s", out)
	assert.NotContains(t, out, "1e+20")
}
