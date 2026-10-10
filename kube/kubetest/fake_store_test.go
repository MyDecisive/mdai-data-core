package kubetest

import (
	"errors"
	"testing"

	"github.com/mydecisive/mdai-data-core/kube"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFakeConfigMapStore_CopiesSeededData(t *testing.T) {
	data := map[string]string{"k": "v"}
	store := NewFakeConfigMapStore().SeedHub("hub", data)

	data["k"] = "changed-after-seeding"

	got, found, err := store.GetEnvConfigMapDataByHubName("hub")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, map[string]string{"k": "v"}, got)
}

func TestFakeConfigMapStore_ReturnsCopies(t *testing.T) {
	const (
		hubName = "hub"
		cmName  = "hub-cm"
	)
	want := map[string]string{"k": "v"}
	store := NewFakeConfigMapStore().SeedConfigMap(hubName, cmName, kube.EnvConfigMapType, map[string]string{"k": "v"})

	// Modify everything the getters return.
	allHubs, err := store.GetAllHubsToDataMap()
	require.NoError(t, err)
	allHubs[hubName]["k"] = "all-hubs"

	allEnv, err := store.GetAllHubsEnvConfigMapData()
	require.NoError(t, err)
	allEnv[hubName]["k"] = "all-env"

	envData, _, err := store.GetEnvConfigMapDataByHubName(hubName)
	require.NoError(t, err)
	envData["k"] = "by-hub"

	byHub, err := store.GetConfigMapByHubName(hubName)
	require.NoError(t, err)
	byHub.Data["k"] = "configmap-by-hub"

	byName, err := store.GetConfigmapByNameAndNamespace(cmName, "")
	require.NoError(t, err)
	byName.Data["k"] = "configmap-by-name"

	// The store must be unchanged.
	gotEnv, _, err := store.GetEnvConfigMapDataByHubName(hubName)
	require.NoError(t, err)
	assert.Equal(t, want, gotEnv)

	gotAll, err := store.GetAllHubsToDataMap()
	require.NoError(t, err)
	assert.Equal(t, want, gotAll[hubName])

	gotByName, err := store.GetConfigmapByNameAndNamespace(cmName, "")
	require.NoError(t, err)
	assert.Equal(t, want, gotByName.Data)
}

func TestFakeConfigMapStore_SetHubConfigMapsCopies(t *testing.T) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "hub-cm",
			Labels: map[string]string{kube.ConfigMapTypeLabel: kube.EnvConfigMapType},
		},
		Data: map[string]string{"k": "v"},
	}
	store := NewFakeConfigMapStore()
	store.SetHubConfigMaps("hub", []*v1.ConfigMap{cm})

	cm.Data["k"] = "changed-after-set"

	got, found, err := store.GetEnvConfigMapDataByHubName("hub")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, map[string]string{"k": "v"}, got)
}

func TestFakeConfigMapStore_ResetClearsConfigMapsByName(t *testing.T) {
	store := NewFakeConfigMapStore().SeedHub("hub", map[string]string{"k": "v"})

	store.Reset()

	cm, err := store.GetConfigmapByNameAndNamespace("hub-cm", "")
	require.Error(t, err, "Reset should clear ConfigMaps looked up by name")
	assert.True(t, apierrors.IsNotFound(err))
	assert.Nil(t, cm)
}

func TestFakeConfigMapStore_GetByNameNotFound(t *testing.T) {
	store := NewFakeConfigMapStore()

	cm, err := store.GetConfigmapByNameAndNamespace("missing", "first")

	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "should match the real controller's not-found error")
	assert.EqualError(t, err, `failed to get configmap first/missing: configmap "missing" not found`)
	assert.Nil(t, cm)
}

func TestFakeConfigMapStore_FailGetByNameWith(t *testing.T) {
	injected := errors.New("injected")
	store := NewFakeConfigMapStore().SeedHub("hub", map[string]string{"k": "v"})

	store.FailGetByNameWith(injected)
	_, err := store.GetConfigmapByNameAndNamespace("hub-cm", "")
	require.ErrorIs(t, err, injected)

	// The hub lookup has its own injection point and is unaffected.
	_, err = store.GetConfigMapByHubName("hub")
	require.NoError(t, err)
}
