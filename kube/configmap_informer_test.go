package kube

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newStartedConfmapController(t *testing.T, watchedTypes []string, namespace string, configMaps ...*corev1.ConfigMap) *ConfigMapController {
	t.Helper()

	objects := make([]runtime.Object, 0, len(configMaps))
	for _, configMap := range configMaps {
		objects = append(objects, configMap)
	}

	controller, err := NewConfigMapController(watchedTypes, namespace, fake.NewClientset(objects...), zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, controller.Run())
	t.Cleanup(controller.Stop)

	return controller
}

func TestConfigMapController_GetConfigmapByNameAndNamespace(t *testing.T) {
	t.Parallel()

	configMap1 := newTestConfigMap(
		"mdaihub-first-octant-yolo",
		"first",
		"mdaihub-first",
		OctantConnectionsConfigMapType,
		map[string]string{"first_manual_variable": "boolean"},
	)
	configMap2 := newTestConfigMap(
		"mdaihub-second-octant-yolo",
		"second",
		"mdaihub-second",
		OctantConnectionsConfigMapType,
		map[string]string{"second_manual_variable": "string"},
	)
	configMap3 := newTestConfigMap(
		"mdaihub-second-octant-swag",
		"second",
		"mdaihub-third",
		OctantConnectionsConfigMapType,
		map[string]string{"third_manual_variable": "string"},
	)

	testCases := []struct {
		description      string
		namespace        string
		configMaps       []*corev1.ConfigMap
		expectedConfmaps map[*corev1.ConfigMap]bool
	}{
		{
			description: "only caching confmaps for provided namespace",
			namespace:   "second",
			configMaps:  []*corev1.ConfigMap{configMap1, configMap2, configMap3},
			expectedConfmaps: map[*corev1.ConfigMap]bool{
				configMap1: false,
				configMap2: true,
				configMap3: true,
			},
		},
		{
			description: "namespace all caches for any namespace",
			namespace:   corev1.NamespaceAll,
			configMaps:  []*corev1.ConfigMap{configMap1, configMap2, configMap3},
			expectedConfmaps: map[*corev1.ConfigMap]bool{
				configMap1: true,
				configMap2: true,
				configMap3: true,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()

			controller := newStartedConfmapController(t, []string{OctantConnectionsConfigMapType}, tt.namespace, tt.configMaps...)

			for confMap, shouldFind := range tt.expectedConfmaps {
				requireEventually(t, func(c *assert.CollectT) {
					cm, err := controller.GetConfigmapByNameAndNamespace(confMap.Name, confMap.Namespace)
					if shouldFind {
						assert.NoError(c, err)
						assert.NotNil(t, cm)
					} else {
						assert.Error(c, err)
						assert.Nil(t, cm)
					}
				})
			}
		})
	}
}

func TestConfigMapController_NonExistentCmType(t *testing.T) {
	t.Parallel()

	controller, err := NewConfigMapController(
		[]string{"hub-nonexistent-cm-type"},
		"second",
		fake.NewClientset(),
		zap.NewNop(),
	)
	require.Nil(t, controller)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported ConfigMap type")
}
