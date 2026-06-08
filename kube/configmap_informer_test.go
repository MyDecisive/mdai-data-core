package kube

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func newStartedConfmapController(t *testing.T, watchedTypes []string, namespace string, configMaps ...*corev1.ConfigMap) (*ConfigMapController, kubernetes.Interface) {
	t.Helper()

	objects := make([]runtime.Object, 0, len(configMaps))
	for _, configMap := range configMaps {
		objects = append(objects, configMap)
	}

	k8sClient := fake.NewClientset(objects...)
	controller, err := NewConfigMapController(watchedTypes, namespace, k8sClient, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, controller.Run())
	t.Cleanup(controller.Stop)

	return controller, k8sClient
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

			controller, _ := newStartedConfmapController(t, []string{OctantConnectionsConfigMapType}, tt.namespace, tt.configMaps...)

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

func TestConfigMapController_CreateConfigMap(t *testing.T) {
	t.Parallel()

	configMap1 := newTestConfigMap(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantConnectionsConfigMapType,
		map[string]string{"first_manual_variable": "boolean"},
	)

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		controller, k8sClient := newStartedConfmapController(t, []string{OctantConnectionsConfigMapType}, "bestNamespace")

		// verify no configmaps exist yet
		cmList, err := k8sClient.CoreV1().ConfigMaps("bestNamespace").List(t.Context(), metav1.ListOptions{})
		require.NoError(t, err)
		require.Empty(t, cmList.Items)

		require.NoError(t, controller.CreateConfigMap(t.Context(), "bestNamespace", configMap1))

		// verify the single cm we created
		theCM, err := k8sClient.CoreV1().ConfigMaps("bestNamespace").Get(t.Context(), configMap1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, configMap1.Name, theCM.Name)
	})
}

func TestConfigMapController_UpdateConfigMap(t *testing.T) {
	t.Parallel()

	configMap1 := newTestConfigMap(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantConnectionsConfigMapType,
		map[string]string{"first_manual_variable": "boolean"},
	)
	configMap2 := newTestConfigMap(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantConnectionsConfigMapType,
		map[string]string{"first_manual_variable": "boolean", "second_manual_variable": "string"},
	)

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		controller, k8sClient := newStartedConfmapController(t, []string{OctantConnectionsConfigMapType}, "bestNamespace", configMap1)

		// verify the CM exists with the single data key
		theCM, err := k8sClient.CoreV1().ConfigMaps("bestNamespace").Get(t.Context(), configMap1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Len(t, theCM.Data, 1)
		assert.Contains(t, theCM.Data, "first_manual_variable")

		require.NoError(t, controller.UpdateConfigMap(t.Context(), "bestNamespace", configMap2))

		// verify the new variable key was added/updated
		theCM, err = k8sClient.CoreV1().ConfigMaps("bestNamespace").Get(t.Context(), configMap1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Len(t, theCM.Data, 2)
		assert.Contains(t, theCM.Data, "second_manual_variable")
	})
}
