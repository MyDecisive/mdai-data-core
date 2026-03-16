package ValkeyAdapter

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
	r1, r2 := hubKey+refs[0], hubKey+refs[1]

	client.
		EXPECT().
		Do(ctx,
			vmock.Match("PRIORITYLIST.GETORCREATE", key, r1, r2),
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
			vmock.Match("PRIORITYLIST.GETORCREATE", key, r1, r2),
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
	r1, r2 := "variable/hub/ref1", "variable/hub/ref2"

	client.
		EXPECT().
		Do(ctx,
			vmock.Match("PRIORITYLIST.GET", key),
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
			vmock.Match("PRIORITYLIST.GET", key),
		).
		Return(vmock.Result(vmock.ValkeyError("WRONGTYPE_OR_NOTFOUND")))

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
			vmock.Match("HASHSET.GETORCREATE", key, strKey, setKey),
		).
		Return(vmock.Result(vmock.ValkeyBlobString(wantVal)))

	got, found, err := adapter.GetOrCreateMetaHashSet(ctx, varKey, "hub", strKeyInput, setKeyInput)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, wantVal, got)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match("HASHSET.GETORCREATE", key, strKey, setKey),
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
			vmock.Match("HASHSET.LOOKUP", key),
		).
		Return(vmock.Result(vmock.ValkeyBlobString(wantVal)))

	got, found, err := adapter.GetMetaHashSet(ctx, varKey, "hub")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, wantVal, got)

	client.
		EXPECT().
		Do(ctx,
			vmock.Match("HASHSET.LOOKUP", key),
		).
		Return(vmock.Result(vmock.ValkeyError("WRONGTYPE_OR_NOTFOUND")))

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
