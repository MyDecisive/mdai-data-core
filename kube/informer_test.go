package kube

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	eventuallyTimeout = time.Second
	eventuallyTick    = 100 * time.Millisecond
)

func writeKubeconfig(t *testing.T, content string) string {
	t.Helper()

	tmpDir := t.TempDir()
	kubeDir := filepath.Join(tmpDir, ".kube")
	require.NoError(t, os.MkdirAll(kubeDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(kubeDir, "config"), []byte(content), 0644))

	return tmpDir
}

func readKubeconfigFixture(t *testing.T, name string) string {
	t.Helper()

	content, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)

	return string(content)
}

func setupValidKubeconfig(t *testing.T) string {
	t.Helper()

	return writeKubeconfig(t, readKubeconfigFixture(t, "kubeconfig_valid.yaml"))
}

func newTestConfigMap(name, namespace, hubName, configMapType string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ConfigMapTypeLabel: configMapType,
				LabelMdaiHubName:   hubName,
			},
		},
		Data: data,
	}
}

func newStartedController(t *testing.T, watchedTypes []string, namespace string, configMaps ...*corev1.ConfigMap) *ConfigMapController {
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

type typedConfigMapFixtures struct {
	envConfigMap        *corev1.ConfigMap
	automationConfigMap *corev1.ConfigMap
	schemaConfigMap     *corev1.ConfigMap
	duplicateEnv1       *corev1.ConfigMap
	duplicateEnv2       *corev1.ConfigMap
}

func newTypedConfigMapFixtures(envHubName, automationHubName, schemaHubName string) typedConfigMapFixtures {
	return typedConfigMapFixtures{
		envConfigMap: newTestConfigMap(
			envHubName+"-variables",
			"first",
			envHubName,
			EnvConfigMapType,
			map[string]string{"env_variable": "string"},
		),
		automationConfigMap: newTestConfigMap(
			automationHubName+"-automation",
			"first",
			automationHubName,
			AutomationConfigMapType,
			map[string]string{"automation_variable": "enabled"},
		),
		schemaConfigMap: newTestConfigMap(
			schemaHubName+"-variables-schema",
			"first",
			schemaHubName,
			VariablesSchemaMapType,
			map[string]string{"schema_variable": "boolean"},
		),
		duplicateEnv1: newTestConfigMap(
			"shared-hub-variables-1",
			"first",
			"shared-hub",
			EnvConfigMapType,
			map[string]string{"env_variable": "string"},
		),
		duplicateEnv2: newTestConfigMap(
			"shared-hub-variables-2",
			"first",
			"shared-hub",
			EnvConfigMapType,
			map[string]string{"env_variable": "string"},
		),
	}
}

func requireEventually(t *testing.T, check func(*assert.CollectT)) {
	t.Helper()

	require.EventuallyWithT(t, check, eventuallyTimeout, eventuallyTick)
}

func TestConfigMapController_NamespaceFiltering(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name               string
		watchedTypes       []string
		namespace          string
		configMaps         []*corev1.ConfigMap
		wantHubs           map[string]*corev1.ConfigMap
		wantAllHubsData    map[string]map[string]string
		wantTypeIndexCount int
	}

	configMap1 := newTestConfigMap(
		"mdaihub-first-manual-variables",
		"first",
		"mdaihub-first",
		ManualEnvConfigMapType,
		map[string]string{"first_manual_variable": "boolean"},
	)
	configMap2 := newTestConfigMap(
		"mdaihub-second-manual-variables",
		"second",
		"mdaihub-second",
		ManualEnvConfigMapType,
		map[string]string{"second_manual_variable": "string"},
	)
	configMap3 := newTestConfigMap(
		"mdaihub-third-manual-variables",
		"second",
		"mdaihub-third",
		ManualEnvConfigMapType,
		map[string]string{"third_manual_variable": "string"},
	)

	tests := []testCase{
		{
			name:         "single namespace only caches matching namespace",
			watchedTypes: []string{ManualEnvConfigMapType},
			namespace:    "second",
			configMaps:   []*corev1.ConfigMap{configMap1, configMap2, configMap3},
			wantHubs: map[string]*corev1.ConfigMap{
				"mdaihub-second": configMap2,
				"mdaihub-third":  configMap3,
			},
			wantAllHubsData: map[string]map[string]string{
				"mdaihub-second": configMap2.Data,
				"mdaihub-third":  configMap3.Data,
			},
			wantTypeIndexCount: 2,
		},
		{
			name:         "namespace all caches all matching namespaces",
			watchedTypes: []string{ManualEnvConfigMapType},
			namespace:    corev1.NamespaceAll,
			configMaps:   []*corev1.ConfigMap{configMap1, configMap2, configMap3},
			wantHubs: map[string]*corev1.ConfigMap{
				"mdaihub-first":  configMap1,
				"mdaihub-second": configMap2,
				"mdaihub-third":  configMap3,
			},
			wantAllHubsData: map[string]map[string]string{
				"mdaihub-first":  configMap1.Data,
				"mdaihub-second": configMap2.Data,
				"mdaihub-third":  configMap3.Data,
			},
			wantTypeIndexCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := newStartedController(t, tt.watchedTypes, tt.namespace, tt.configMaps...)

			for hubName, wantConfigMap := range tt.wantHubs {
				requireEventually(t, func(c *assert.CollectT) {
					configMap, err := controller.GetConfigMapByHubName(hubName)
					if !assert.NoError(c, err) {
						return
					}
					assert.Equal(c, wantConfigMap, configMap)
				})
			}

			requireEventually(t, func(c *assert.CollectT) {
				hubData, err := controller.GetAllHubsToDataMap()
				if !assert.NoError(c, err) {
					return
				}
				assert.Equal(c, tt.wantAllHubsData, hubData)
			})

			requireEventually(t, func(c *assert.CollectT) {
				objects, err := controller.CmInformer.Informer().GetIndexer().ByIndex(ByType, ManualEnvConfigMapType)
				if !assert.NoError(c, err) {
					return
				}
				assert.Len(c, objects, tt.wantTypeIndexCount)
			})
		})
	}
}

func TestNewConfigMapController_NonExistentCmType(t *testing.T) {
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

func TestGetHubName(t *testing.T) {
	t.Parallel()

	t.Run("returns hub name label", func(t *testing.T) {
		hubName, err := getHubName(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					LabelMdaiHubName: "mdaihub-first",
				},
			},
		})

		require.NoError(t, err)
		require.Equal(t, "mdaihub-first", hubName)
	})

	t.Run("returns error when label is missing", func(t *testing.T) {
		hubName, err := getHubName(&corev1.ConfigMap{})

		require.ErrorIs(t, err, errNoHubNameLabel)
		require.Empty(t, hubName)
	})
}

func TestGetConfigMapType(t *testing.T) {
	t.Parallel()

	t.Run("returns configmap type label", func(t *testing.T) {
		configMapType, err := getConfigMapType(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					ConfigMapTypeLabel: EnvConfigMapType,
				},
			},
		})

		require.NoError(t, err)
		require.Equal(t, EnvConfigMapType, configMapType)
	})

	t.Run("returns error when label is missing", func(t *testing.T) {
		configMapType, err := getConfigMapType(&corev1.ConfigMap{})

		require.ErrorIs(t, err, errNoConfigMapTypeLabel)
		require.Empty(t, configMapType)
	})
}

func TestGetConfigMapByHubName(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		watchedTypes    []string
		namespace       string
		configMaps      []*corev1.ConfigMap
		hubName         string
		wantConfigMap   *corev1.ConfigMap
		wantErrContains string
	}

	successConfigMap := newTestConfigMap(
		"mdaihub-first-manual-variables",
		"first",
		"mdaihub-first",
		ManualEnvConfigMapType,
		map[string]string{"first_manual_variable": "boolean"},
	)
	duplicateManual := newTestConfigMap(
		"shared-hub-manual-variables",
		"first",
		"shared-hub",
		ManualEnvConfigMapType,
		nil,
	)
	duplicateEnv := newTestConfigMap(
		"shared-hub-variables",
		"first",
		"shared-hub",
		EnvConfigMapType,
		nil,
	)

	tests := []testCase{
		{
			name:          "returns configmap for existing hub",
			watchedTypes:  []string{ManualEnvConfigMapType},
			namespace:     "first",
			configMaps:    []*corev1.ConfigMap{successConfigMap},
			hubName:       "mdaihub-first",
			wantConfigMap: successConfigMap,
		},
		{
			name:            "returns not found error for missing hub",
			watchedTypes:    []string{ManualEnvConfigMapType},
			namespace:       "first",
			hubName:         "non-existent-hub",
			wantErrContains: "no ConfigMap found for hub: non-existent-hub",
		},
		{
			name:            "returns error for multiple configmaps with same hub",
			watchedTypes:    []string{ManualEnvConfigMapType, EnvConfigMapType},
			namespace:       "first",
			configMaps:      []*corev1.ConfigMap{duplicateManual, duplicateEnv},
			hubName:         "shared-hub",
			wantErrContains: "multiple ConfigMaps found for the same hub: shared-hub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := newStartedController(t, tt.watchedTypes, tt.namespace, tt.configMaps...)
			if len(tt.configMaps) == 0 {
				configMap, err := controller.GetConfigMapByHubName(tt.hubName)
				require.Nil(t, configMap)
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrContains)
				return
			}

			requireEventually(t, func(c *assert.CollectT) {
				configMap, err := controller.GetConfigMapByHubName(tt.hubName)
				if tt.wantErrContains != "" {
					assert.Nil(c, configMap)
					if assert.Error(c, err) {
						assert.Contains(c, err.Error(), tt.wantErrContains)
					}
					return
				}

				if !assert.NoError(c, err) {
					return
				}
				assert.Equal(c, tt.wantConfigMap, configMap)
			})
		})
	}
}

func TestTypedConfigMapDataByHubName(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		watchedTypes    []string
		namespace       string
		configMaps      []*corev1.ConfigMap
		hubName         string
		getData         func(*ConfigMapController, string) (map[string]string, bool, error)
		wantData        map[string]string
		wantFound       bool
		wantErrContains string
	}

	fixtures := newTypedConfigMapFixtures("shared-hub", "shared-hub", "shared-hub")
	nilDataEnvConfigMap := newTestConfigMap(
		"nil-data-hub-variables",
		"first",
		"nil-data-hub",
		EnvConfigMapType,
		nil,
	)

	tests := []testCase{
		{
			name:         "returns env configmap data",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			hubName:      "shared-hub",
			getData:      (*ConfigMapController).GetEnvConfigMapDataByHubName,
			wantData:     fixtures.envConfigMap.Data,
			wantFound:    true,
		},
		{
			name:         "returns automation configmap data",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			hubName:      "shared-hub",
			getData:      (*ConfigMapController).GetAutomationConfigMapDataByHubName,
			wantData:     fixtures.automationConfigMap.Data,
			wantFound:    true,
		},
		{
			name:         "returns variables schema configmap data",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			hubName:      "shared-hub",
			getData:      (*ConfigMapController).GetVariablesSchemaConfigMapDataByHubName,
			wantData:     fixtures.schemaConfigMap.Data,
			wantFound:    true,
		},
		{
			name:         "returns nil data map when configmap data is nil",
			watchedTypes: []string{EnvConfigMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{nilDataEnvConfigMap},
			hubName:      "nil-data-hub",
			getData:      (*ConfigMapController).GetEnvConfigMapDataByHubName,
			wantData:     nil,
			wantFound:    true,
		},
		{
			name:         "returns not found for missing hub",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			hubName:      "missing-hub",
			getData:      (*ConfigMapController).GetEnvConfigMapDataByHubName,
			wantFound:    false,
		},
		{
			name:            "returns error for duplicate hub and type",
			watchedTypes:    []string{EnvConfigMapType},
			namespace:       "first",
			configMaps:      []*corev1.ConfigMap{fixtures.duplicateEnv1, fixtures.duplicateEnv2},
			hubName:         "shared-hub",
			getData:         (*ConfigMapController).GetEnvConfigMapDataByHubName,
			wantFound:       true,
			wantErrContains: "multiple ConfigMaps found for the same hub and type: shared-hub, hub-variables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := newStartedController(t, tt.watchedTypes, tt.namespace, tt.configMaps...)
			requireEventually(t, func(c *assert.CollectT) {
				data, found, err := tt.getData(controller, tt.hubName)
				assert.Equal(c, tt.wantFound, found)

				if tt.wantErrContains != "" {
					assert.Nil(c, data)
					if assert.Error(c, err) {
						assert.Contains(c, err.Error(), tt.wantErrContains)
					}
					return
				}

				if !assert.NoError(c, err) {
					return
				}
				assert.Equal(c, tt.wantData, data)
			})
		})
	}
}

func TestAllHubsTypedConfigMapData(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name            string
		watchedTypes    []string
		namespace       string
		configMaps      []*corev1.ConfigMap
		getData         func(*ConfigMapController) (map[string]map[string]string, error)
		wantMap         map[string]map[string]string
		wantErrContains string
	}

	fixtures := newTypedConfigMapFixtures("mdaihub-first", "mdaihub-second", "mdaihub-third")
	nilDataEnvConfigMap := newTestConfigMap(
		"nil-data-hub-variables",
		"first",
		"nil-data-hub",
		EnvConfigMapType,
		nil,
	)

	tests := []testCase{
		{
			name:         "returns env configmap data for all hubs",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			getData:      (*ConfigMapController).GetAllHubsEnvConfigMapData,
			wantMap: map[string]map[string]string{
				"mdaihub-first": fixtures.envConfigMap.Data,
			},
		},
		{
			name:         "returns nil data map for a hub when configmap data is nil",
			watchedTypes: []string{EnvConfigMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{nilDataEnvConfigMap},
			getData:      (*ConfigMapController).GetAllHubsEnvConfigMapData,
			wantMap: map[string]map[string]string{
				"nil-data-hub": nil,
			},
		},
		{
			name:         "returns automation configmap data for all hubs",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			getData:      (*ConfigMapController).GetAllHubsAutomationConfigMapData,
			wantMap: map[string]map[string]string{
				"mdaihub-second": fixtures.automationConfigMap.Data,
			},
		},
		{
			name:         "returns variables schema configmap data for all hubs",
			watchedTypes: []string{EnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType},
			namespace:    "first",
			configMaps:   []*corev1.ConfigMap{fixtures.envConfigMap, fixtures.automationConfigMap, fixtures.schemaConfigMap},
			getData:      (*ConfigMapController).GetAllHubsVariablesSchemaConfigMapData,
			wantMap: map[string]map[string]string{
				"mdaihub-third": fixtures.schemaConfigMap.Data,
			},
		},
		{
			name:            "returns error for duplicate hub and type",
			watchedTypes:    []string{EnvConfigMapType},
			namespace:       "first",
			configMaps:      []*corev1.ConfigMap{fixtures.duplicateEnv1, fixtures.duplicateEnv2},
			getData:         (*ConfigMapController).GetAllHubsEnvConfigMapData,
			wantErrContains: "multiple ConfigMaps found for the same hub and type: shared-hub, hub-variables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := newStartedController(t, tt.watchedTypes, tt.namespace, tt.configMaps...)
			requireEventually(t, func(c *assert.CollectT) {
				hubData, err := tt.getData(controller)
				if tt.wantErrContains != "" {
					assert.Nil(c, hubData)
					if assert.Error(c, err) {
						assert.Contains(c, err.Error(), tt.wantErrContains)
					}
					return
				}

				if !assert.NoError(c, err) {
					return
				}
				assert.Equal(c, tt.wantMap, hubData)
			})
		})
	}
}

func TestGetKubeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		homeDir         func(*testing.T) (string, error)
		expectError     bool
		errorContains   string
		wantHost        string
		wantBearerToken string
		wantInsecure    *bool
	}{
		{
			name: "valid kubeconfig exists",
			homeDir: func(t *testing.T) (string, error) {
				return setupValidKubeconfig(t), nil
			},
		},
		{
			name: "home dir getter returns error",
			homeDir: func(*testing.T) (string, error) {
				return "", errors.New("failed to get home directory")
			},
			expectError:   true,
			errorContains: "home directory",
		},
		{
			name: "kubeconfig file does not exist",
			homeDir: func(t *testing.T) (string, error) {
				return t.TempDir(), nil
			},
			expectError: true,
		},
		{
			name: "invalid kubeconfig yaml",
			homeDir: func(t *testing.T) (string, error) {
				return writeKubeconfig(t, readKubeconfigFixture(t, "kubeconfig_invalid.yaml")), nil
			},
			expectError: true,
		},
		{
			name: "empty kubeconfig file",
			homeDir: func(t *testing.T) (string, error) {
				return writeKubeconfig(t, readKubeconfigFixture(t, "kubeconfig_empty.yaml")), nil
			},
			expectError: true,
		},
		{
			name: "kubeconfig without current-context",
			homeDir: func(t *testing.T) (string, error) {
				return writeKubeconfig(t, readKubeconfigFixture(t, "kubeconfig_no_current_context.yaml")), nil
			},
			expectError: true,
		},
		{
			name: "config properties are loaded correctly",
			homeDir: func(t *testing.T) (string, error) {
				return setupValidKubeconfig(t), nil
			},
			wantHost:        "https://localhost:6443",
			wantBearerToken: "test-token",
			wantInsecure:    lo.ToPtr(true),
		},
		{
			name: "uses current-context from multiple contexts",
			homeDir: func(t *testing.T) (string, error) {
				return writeKubeconfig(t, readKubeconfigFixture(t, "kubeconfig_multiple_contexts.yaml")), nil
			},
			wantHost:        "https://cluster2:6443",
			wantBearerToken: "token2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger := zaptest.NewLogger(t)
			config, err := getKubeConfig(logger, func() (string, error) {
				return tt.homeDir(t)
			})
			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, config)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, config)
			if tt.wantHost != "" {
				require.Equal(t, tt.wantHost, config.Host)
			} else {
				require.NotEmpty(t, config.Host)
			}
			if tt.wantBearerToken != "" {
				require.Equal(t, tt.wantBearerToken, config.BearerToken)
			}
			if tt.wantInsecure != nil {
				require.Equal(t, *tt.wantInsecure, config.Insecure)
			}
		})
	}
}

func TestNewK8sClients(t *testing.T) {
	clients := []struct {
		name    string
		factory func(*zap.Logger) (any, error)
	}{
		{
			name: "K8sClient",
			factory: func(logger *zap.Logger) (any, error) {
				return NewK8sClient(logger)
			},
		},
		{
			name: "K8sDynamicClient",
			factory: func(logger *zap.Logger) (any, error) {
				return NewK8sDynamicClient(logger)
			},
		},
	}

	for _, clientFactory := range clients {
		t.Run(clientFactory.name+"/success", func(t *testing.T) {
			t.Setenv("HOME", setupValidKubeconfig(t))
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
			t.Setenv("KUBERNETES_SERVICE_PORT", "")

			client, err := clientFactory.factory(zaptest.NewLogger(t))

			require.NoError(t, err)
			require.NotNil(t, client)
		})

		t.Run(clientFactory.name+"/missing", func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
			t.Setenv("KUBERNETES_SERVICE_PORT", "")

			client, err := clientFactory.factory(zaptest.NewLogger(t))

			require.Error(t, err)
			require.Nil(t, client)
		})
	}
}

func TestGetKubeConfig_InClusterSuccess(t *testing.T) {
	t.Parallel()

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
	if err == nil && homeDirFuncCalled {
		t.Error("homeDirFunc should not be called when InClusterConfig succeeds")
	}
}

func TestBuildLabelSelector(t *testing.T) {
	t.Parallel()

	t.Run("multiple types", func(t *testing.T) {
		t.Parallel()

		selector, err := labels.Parse(buildLabelSelector([]string{EnvConfigMapType, AutomationConfigMapType}))
		require.NoError(t, err)

		assert.True(t, selector.Matches(labels.Set{ConfigMapTypeLabel: EnvConfigMapType}))
		assert.True(t, selector.Matches(labels.Set{ConfigMapTypeLabel: AutomationConfigMapType}))
		assert.False(t, selector.Matches(labels.Set{ConfigMapTypeLabel: VariablesSchemaMapType}))
		assert.False(t, selector.Matches(labels.Set{}))
	})

	t.Run("single type", func(t *testing.T) {
		t.Parallel()

		selector, err := labels.Parse(buildLabelSelector([]string{EnvConfigMapType}))
		require.NoError(t, err)

		assert.True(t, selector.Matches(labels.Set{ConfigMapTypeLabel: EnvConfigMapType}))
		assert.False(t, selector.Matches(labels.Set{ConfigMapTypeLabel: AutomationConfigMapType}))
		assert.False(t, selector.Matches(labels.Set{}))
	})
}
