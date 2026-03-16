package kube

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	dummyHomeDir = "will-be-replaced-by-setup"
)

func TestNewConfigMapController_SingleNs(t *testing.T) {
	logger := zap.NewNop()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"second_manual_variable": "string",
		},
	}
	configMap3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"third_manual_variable": "string",
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2, configMap3)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, "second", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-second")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap2, cm)
	}, time.Second, 100*time.Millisecond, "Expected second hub ConfigMap to eventually match")

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-third")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap3, cm)
	}, time.Second, 100*time.Millisecond, "Expected third hub ConfigMap to eventually match")

	assert.Eventually(t, func() bool {
		objs, err := cmController.CmInformer.Informer().GetIndexer().ByIndex(ByType, ManualEnvConfigMapType)
		if err != nil {
			return false
		}
		return len(objs) == 2
	}, time.Second, 100*time.Millisecond, "Expected type index to include namespace-filtered config maps")

}
func TestNewConfigMapController_MultipleNs(t *testing.T) {
	var logger = zap.NewNop()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"second_manual_variable": "string",
		},
	}
	configMap3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"third_manual_variable": "string",
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2, configMap3)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, corev1.NamespaceAll, clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	defer cmController.Stop()
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-first")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap1, cm)
	}, time.Second, 100*time.Millisecond, "Expected first hub ConfigMap to eventually match")

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-second")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap2, cm)
	}, time.Second, 100*time.Millisecond, "Expected second hub ConfigMap to eventually match")

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-third")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap3, cm)
	}, time.Second, 100*time.Millisecond, "Expected third hub ConfigMap to eventually match")

}

func TestNewConfigMapController_NonExistentCmType(t *testing.T) {
	var logger = zap.NewNop()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}

	clientset := fake.NewClientset(configMap1)

	cmController, err := NewConfigMapController([]string{"hub-nonexistent-cm-type"}, "second", clientset, logger)
	require.Nil(t, cmController)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported ConfigMap type")
}

func TestGetAllHubsToDataMap(t *testing.T) {
	var logger = zap.NewNop()

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable": "boolean",
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"second_manual_variable": "string",
		},
	}
	configMap3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-manual-variables",
			Namespace: "second",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"third_manual_variable": "string",
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2, configMap3)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, "second", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	expectedHubData := map[string]map[string]string{
		"mdaihub-second": {
			"second_manual_variable": "string",
		},
		"mdaihub-third": {
			"third_manual_variable": "string",
		},
	}

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetAllHubsToDataMap()
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(expectedHubData, hubData)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")
}

func TestGetAllHubsToDataMapByType(t *testing.T) {
	var logger = zap.NewNop()

	manualConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"manual_variable": "string",
		},
	}
	schemaConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-variables-schema",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: VariablesSchemaMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"schema_variable": "boolean",
		},
	}
	otherManualConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"other_manual_variable": "int",
		},
	}

	clientset := fake.NewClientset(manualConfigMap, schemaConfigMap, otherManualConfigMap)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType, VariablesSchemaMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		hubData, err := cmController.getAllHubsToDataMapByType(ManualEnvConfigMapType)
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(map[string]map[string]string{
			"mdaihub-first": {
				"manual_variable": "string",
			},
			"mdaihub-second": {
				"other_manual_variable": "int",
			},
		}, hubData)
	}, time.Second, 100*time.Millisecond, "Expected typed hub data to include only manual variables")

	assert.Eventually(t, func() bool {
		hubData, err := cmController.getAllHubsToDataMapByType(VariablesSchemaMapType)
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(map[string]map[string]string{
			"mdaihub-first": {
				"schema_variable": "boolean",
			},
		}, hubData)
	}, time.Second, 100*time.Millisecond, "Expected typed hub data to include only schema variables")
}

func TestGetAllHubsToDataMapByType_MultipleFound(t *testing.T) {
	var logger = zap.NewNop()
	const hubName = "shared-hub"

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-hub-manual-variables-1",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-hub-manual-variables-2",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		hubData, err := cmController.getAllHubsToDataMapByType(ManualEnvConfigMapType)
		if hubData != nil {
			return false
		}
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), "multiple ConfigMaps found for the same hub and type: shared-hub, hub-manual-variables")
	}, time.Second, 100*time.Millisecond, "Expected error for duplicate config maps with the same hub and type")
}

func TestGetAllHubsTypedConfigMapDataHelpers(t *testing.T) {
	var logger = zap.NewNop()

	envConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: EnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"env_variable": "string",
		},
	}
	automationConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-automation",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: AutomationConfigMapType,
				LabelMdaiHubName:   "mdaihub-second",
			},
		},
		Data: map[string]string{
			"automation_variable": "enabled",
		},
	}
	schemaConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-third-variables-schema",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: VariablesSchemaMapType,
				LabelMdaiHubName:   "mdaihub-third",
			},
		},
		Data: map[string]string{
			"schema_variable": "boolean",
		},
	}

	clientset := fake.NewClientset(envConfigMap, automationConfigMap, schemaConfigMap)

	cmController, err := NewConfigMapController([]string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetAllHubsEnvConfigMapData()
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(map[string]map[string]string{
			"mdaihub-first": envConfigMap.Data,
		}, hubData)
	}, time.Second, 100*time.Millisecond, "Expected env bulk helper to return data")

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetAllHubsAutomationConfigMapData()
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(map[string]map[string]string{
			"mdaihub-second": automationConfigMap.Data,
		}, hubData)
	}, time.Second, 100*time.Millisecond, "Expected automation bulk helper to return data")

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetAllHubsVariablesSchemaConfigMapData()
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(map[string]map[string]string{
			"mdaihub-third": schemaConfigMap.Data,
		}, hubData)
	}, time.Second, 100*time.Millisecond, "Expected variables schema bulk helper to return data")
}

func TestGetConfigMapByHubName_NotFound(t *testing.T) {
	var logger = zap.NewNop()
	clientset := fake.NewClientset()

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("non-existent-hub")
		if cm != nil {
			return false
		}
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), "no ConfigMap found for hub: non-existent-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for non-existent hub")
}

func TestGetConfigMapByHubName_MultipleFound(t *testing.T) {
	var logger = zap.NewNop()
	const hubName = "shared-hub"

	configMap1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}
	configMap2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-second-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: EnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
	}

	clientset := fake.NewClientset(configMap1, configMap2)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType, EnvConfigMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName(hubName)
		if cm != nil {
			return false
		}
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), "multiple ConfigMaps found for the same hub: shared-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for multiple config maps")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.getConfigMapDataByHubNameAndType(hubName, ManualEnvConfigMapType)
		if err != nil || !found {
			return false
		}
		return assert.ObjectsAreEqual(configMap1.Data, data)
	}, time.Second, 100*time.Millisecond, "Expected typed lookup to return manual variables config map data")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.getConfigMapDataByHubNameAndType(hubName, EnvConfigMapType)
		if err != nil || !found {
			return false
		}
		return assert.ObjectsAreEqual(configMap2.Data, data)
	}, time.Second, 100*time.Millisecond, "Expected typed lookup to return env variables config map data")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.getConfigMapDataByHubNameAndType(hubName, VariablesSchemaMapType)
		if data != nil {
			return false
		}
		if err != nil {
			return false
		}
		return !found
	}, time.Second, 100*time.Millisecond, "Expected typed lookup to report not found when the requested type is not cached")
}

func TestGetTypedConfigMapDataByHubNameHelpers(t *testing.T) {
	var logger = zap.NewNop()
	const hubName = "shared-hub"

	envConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-hub-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: EnvConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
		Data: map[string]string{
			"env_variable": "string",
		},
	}
	automationConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-hub-automation",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: AutomationConfigMapType,
				LabelMdaiHubName:   hubName,
			},
		},
		Data: map[string]string{
			"automation_variable": "enabled",
		},
	}
	schemaConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shared-hub-variables-schema",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: VariablesSchemaMapType,
				LabelMdaiHubName:   hubName,
			},
		},
		Data: map[string]string{
			"schema_variable": "boolean",
		},
	}

	clientset := fake.NewClientset(envConfigMap, automationConfigMap, schemaConfigMap)

	cmController, err := NewConfigMapController([]string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	assert.Eventually(t, func() bool {
		data, found, err := cmController.GetEnvConfigMapDataByHubName(hubName)
		if err != nil || !found {
			return false
		}
		return assert.ObjectsAreEqual(envConfigMap.Data, data)
	}, time.Second, 100*time.Millisecond, "Expected env config map helper to return data")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.GetAutomationConfigMapDataByHubName(hubName)
		if err != nil || !found {
			return false
		}
		return assert.ObjectsAreEqual(automationConfigMap.Data, data)
	}, time.Second, 100*time.Millisecond, "Expected automation config map helper to return data")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.GetVariablesSchemaConfigMapDataByHubName(hubName)
		if err != nil || !found {
			return false
		}
		return assert.ObjectsAreEqual(schemaConfigMap.Data, data)
	}, time.Second, 100*time.Millisecond, "Expected variables schema config map helper to return data")

	assert.Eventually(t, func() bool {
		data, found, err := cmController.GetEnvConfigMapDataByHubName("missing-hub")
		if data != nil {
			return false
		}
		if err != nil {
			return false
		}
		return !found
	}, time.Second, 100*time.Millisecond, "Expected env config map helper to report not found for a missing hub")
}

func TestGetKubeConfig(t *testing.T) {
	tests := []struct {
		name            string
		homeDirFunc     HomeDirGetterFunc
		setupKubeconfig func(t *testing.T) string // returns path to temp dir
		expectError     bool
		errorContains   string
	}{
		{
			name: "valid kubeconfig exists",
			homeDirFunc: func() (string, error) {
				return dummyHomeDir, nil
			},
			setupKubeconfig: func(t *testing.T) string {
				tmpDir := t.TempDir()
				kubeDir := filepath.Join(tmpDir, ".kube")
				if err := os.MkdirAll(kubeDir, 0755); err != nil {
					t.Fatal(err)
				}

				kubeconfigPath := filepath.Join(kubeDir, "config")
				kubeconfigContent := `
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://localhost:6443
    insecure-skip-tls-verify: true
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
  user:
    token: test-token
`
				if err := os.WriteFile(kubeconfigPath, []byte(kubeconfigContent), 0644); err != nil {
					t.Fatal(err)
				}
				return tmpDir
			},
			expectError: false,
		},
		{
			name: "home dir getter returns error",
			homeDirFunc: func() (string, error) {
				return "", errors.New("failed to get home directory")
			},
			setupKubeconfig: func(t *testing.T) string {
				return "" // not used
			},
			expectError:   true,
			errorContains: "home directory",
		},
		{
			name: "kubeconfig file does not exist",
			homeDirFunc: func() (string, error) {
				return dummyHomeDir, nil
			},
			setupKubeconfig: func(t *testing.T) string {
				// Return a temp dir without creating .kube/config
				return t.TempDir()
			},
			expectError: true,
		},
		{
			name: "invalid kubeconfig yaml",
			homeDirFunc: func() (string, error) {
				return dummyHomeDir, nil
			},
			setupKubeconfig: func(t *testing.T) string {
				tmpDir := t.TempDir()
				kubeDir := filepath.Join(tmpDir, ".kube")
				if err := os.MkdirAll(kubeDir, 0755); err != nil {
					t.Fatal(err)
				}

				kubeconfigPath := filepath.Join(kubeDir, "config")
				// Write invalid YAML
				if err := os.WriteFile(kubeconfigPath, []byte("invalid: yaml: content: [[["), 0644); err != nil {
					t.Fatal(err)
				}
				return tmpDir
			},
			expectError: true,
		},
		{
			name: "empty kubeconfig file",
			homeDirFunc: func() (string, error) {
				return dummyHomeDir, nil
			},
			setupKubeconfig: func(t *testing.T) string {
				tmpDir := t.TempDir()
				kubeDir := filepath.Join(tmpDir, ".kube")
				if err := os.MkdirAll(kubeDir, 0755); err != nil {
					t.Fatal(err)
				}

				kubeconfigPath := filepath.Join(kubeDir, "config")
				if err := os.WriteFile(kubeconfigPath, []byte(""), 0644); err != nil {
					t.Fatal(err)
				}
				return tmpDir
			},
			expectError: true,
		},
		{
			name: "kubeconfig without current-context",
			homeDirFunc: func() (string, error) {
				return dummyHomeDir, nil
			},
			setupKubeconfig: func(t *testing.T) string {
				tmpDir := t.TempDir()
				kubeDir := filepath.Join(tmpDir, ".kube")
				if err := os.MkdirAll(kubeDir, 0755); err != nil {
					t.Fatal(err)
				}

				kubeconfigPath := filepath.Join(kubeDir, "config")
				kubeconfigContent := `
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://localhost:6443
  name: test-cluster
`
				if err := os.WriteFile(kubeconfigPath, []byte(kubeconfigContent), 0644); err != nil {
					t.Fatal(err)
				}
				return tmpDir
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)

			// Set up the test environment
			var homeDirFunc HomeDirGetterFunc
			if tt.setupKubeconfig != nil {
				tmpDir := tt.setupKubeconfig(t)
				// Wrap the original homeDirFunc to return our temp dir
				originalFunc := tt.homeDirFunc
				homeDirFunc = func() (string, error) {
					homeDir, err := originalFunc()
					if err != nil {
						return "", err
					}
					if homeDir == dummyHomeDir {
						return tmpDir, nil
					}
					return homeDir, nil
				}
			} else {
				homeDirFunc = tt.homeDirFunc
			}

			config, err := getKubeConfig(logger, homeDirFunc)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				if config != nil {
					t.Errorf("expected nil config but got: %v", config)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if config == nil {
					t.Errorf("expected config but got nil")
				} else {
					// Validate the config has expected properties
					if config.Host == "" {
						t.Errorf("expected config to have Host set")
					}
				}
			}
		})
	}
}

func TestGetKubeConfig_InClusterSuccess(t *testing.T) {
	// This test documents that if InClusterConfig succeeds,
	// the homeDirFunc should never be called
	logger := zaptest.NewLogger(t)

	homeDirFuncCalled := false
	homeDirFunc := func() (string, error) {
		homeDirFuncCalled = true
		return "", errors.New("should not be called")
	}

	// Note: This test will only pass if running inside a K8s cluster
	// or if InClusterConfig is mocked. In practice, InClusterConfig
	// will fail in a test environment, so homeDirFunc will be called.
	// This test documents the intended behavior.
	_, err := getKubeConfig(logger, homeDirFunc)

	// We expect an error in test environment since we're not in a cluster
	// and homeDirFunc will fail
	if err == nil {
		// If somehow we ARE in a cluster, verify homeDirFunc wasn't called
		if homeDirFuncCalled {
			t.Error("homeDirFunc should not be called when InClusterConfig succeeds")
		}
	}
}

func TestGetKubeConfig_ConfigProperties(t *testing.T) {
	// Test that the returned config has the expected properties
	tmpDir := t.TempDir()
	kubeDir := filepath.Join(tmpDir, ".kube")
	if err := os.MkdirAll(kubeDir, 0755); err != nil {
		t.Fatal(err)
	}

	kubeconfigPath := filepath.Join(kubeDir, "config")
	kubeconfigContent := `
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://test-server:6443
    insecure-skip-tls-verify: true
  name: test-cluster
contexts:
- context:
    cluster: test-cluster
    user: test-user
  name: test-context
current-context: test-context
users:
- name: test-user
  user:
    token: test-token-value
`
	if err := os.WriteFile(kubeconfigPath, []byte(kubeconfigContent), 0644); err != nil {
		t.Fatal(err)
	}

	logger := zaptest.NewLogger(t)
	homeDirFunc := func() (string, error) {
		return tmpDir, nil
	}

	config, err := getKubeConfig(logger, homeDirFunc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify config properties
	if config.Host != "https://test-server:6443" {
		t.Errorf("expected Host to be 'https://test-server:6443', got '%s'", config.Host)
	}

	if config.BearerToken != "test-token-value" {
		t.Errorf("expected BearerToken to be 'test-token-value', got '%s'", config.BearerToken)
	}

	if !config.Insecure {
		t.Error("expected Insecure to be true")
	}
}

func TestGetKubeConfig_MultipleContexts(t *testing.T) {
	// Test kubeconfig with multiple contexts - should use current-context
	tmpDir := t.TempDir()
	kubeDir := filepath.Join(tmpDir, ".kube")
	if err := os.MkdirAll(kubeDir, 0755); err != nil {
		t.Fatal(err)
	}

	kubeconfigPath := filepath.Join(kubeDir, "config")
	kubeconfigContent := `
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://cluster1:6443
    insecure-skip-tls-verify: true
  name: cluster1
- cluster:
    server: https://cluster2:6443
    insecure-skip-tls-verify: true
  name: cluster2
contexts:
- context:
    cluster: cluster1
    user: user1
  name: context1
- context:
    cluster: cluster2
    user: user2
  name: context2
current-context: context2
users:
- name: user1
  user:
    token: token1
- name: user2
  user:
    token: token2
`
	if err := os.WriteFile(kubeconfigPath, []byte(kubeconfigContent), 0644); err != nil {
		t.Fatal(err)
	}

	logger := zaptest.NewLogger(t)
	homeDirFunc := func() (string, error) {
		return tmpDir, nil
	}

	config, err := getKubeConfig(logger, homeDirFunc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should use context2 (current-context)
	if config.Host != "https://cluster2:6443" {
		t.Errorf("expected Host to be 'https://cluster2:6443', got '%s'", config.Host)
	}

	if config.BearerToken != "token2" {
		t.Errorf("expected BearerToken to be 'token2', got '%s'", config.BearerToken)
	}
}
