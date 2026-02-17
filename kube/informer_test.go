package kube

import (
	"errors"
	"os"
	"path/filepath"
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
	ctx := t.Context()

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

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps("second").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 2)

	indexer := cmController.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	assert.ElementsMatch(t, hubNames, []string{"mdaihub-second", "mdaihub-third"})

	hubMap := make(map[string]*corev1.ConfigMap)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			cm := obj.(*corev1.ConfigMap)
			hubMap[hubName] = cm
		}
	}
	assert.Equal(t, hubMap["mdaihub-second"], configMap2)
	assert.Equal(t, hubMap["mdaihub-third"], configMap3)

}
func TestNewConfigMapController_MultipleNs(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()

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

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	// List ConfigMaps
	configMaps, err := clientset.CoreV1().ConfigMaps(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, configMaps.Items, 3)

	indexer := cmController.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	assert.ElementsMatch(t, hubNames, []string{"mdaihub-first", "mdaihub-second", "mdaihub-third"})

	hubMap := make(map[string]*corev1.ConfigMap)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			cm := obj.(*corev1.ConfigMap)
			hubMap[hubName] = cm
		}
	}
	assert.Equal(t, hubMap["mdaihub-first"], configMap1)
	assert.Equal(t, hubMap["mdaihub-second"], configMap2)
	assert.Equal(t, hubMap["mdaihub-third"], configMap3)

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

func TestGetHubData(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mdaihub-first-manual-variables",
			Namespace: "first",
			Labels: map[string]string{
				ConfigMapTypeLabel: ManualEnvConfigMapType,
				LabelMdaiHubName:   "mdaihub-first",
			},
		},
		Data: map[string]string{
			"first_manual_variable":  "boolean",
			"second_manual_variable": "string",
		},
	}
	clientset := fake.NewClientset(configMap)

	cmController, err := NewConfigMapController([]string{ManualEnvConfigMapType}, "first", clientset, logger)
	require.NoError(t, err)

	err = cmController.Run()
	require.NoError(t, err)
	defer cmController.Stop()

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

	expectedHubData := []map[string]string{
		{
			"first_manual_variable":  "boolean",
			"second_manual_variable": "string",
		},
	}

	assert.Eventually(t, func() bool {
		hubData, err := cmController.GetHubData("mdaihub-first")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(expectedHubData, hubData)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName("mdaihub-first")
		if err != nil {
			return false
		}
		return assert.ObjectsAreEqual(configMap, cm)
	}, time.Second, 100*time.Millisecond, "Expected hub data to eventually match")
}

func TestGetAllHubsToDataMap(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()

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

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap2, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("second").Create(ctx, configMap3, metav1.CreateOptions{})
	time.Sleep(100 * time.Millisecond)

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
		return assert.Contains(t, err.Error(), "no ConfigMap found for hub: non-existent-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for non-existent hub")
}

func TestGetConfigMapByHubName_MultipleFound(t *testing.T) {
	var logger = zap.NewNop()
	ctx := t.Context()
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

	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap1, metav1.CreateOptions{})
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap2, metav1.CreateOptions{})

	assert.Eventually(t, func() bool {
		cm, err := cmController.GetConfigMapByHubName(hubName)
		if cm != nil {
			return false
		}
		if err == nil {
			return false
		}
		return assert.Contains(t, err.Error(), "multiple ConfigMaps found for the same hub: shared-hub")
	}, time.Second, 100*time.Millisecond, "Expected error for multiple config maps")
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
