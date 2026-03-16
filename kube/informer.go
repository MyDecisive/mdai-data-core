package kube

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/samber/lo"

	"k8s.io/client-go/dynamic"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ByHubAndType            = "IndexByHubAndType"
	ByType                  = "IndexByType"
	EnvConfigMapType        = "hub-variables"
	AutomationConfigMapType = "hub-automation"
	VariablesSchemaMapType  = "hub-variables-schema"
	LabelMdaiHubName        = "mydecisive.ai/hub-name"
	ConfigMapTypeLabel      = "mydecisive.ai/configmap-type"
	// Deprecated: migrate to VariablesSchemaMapType.
	ManualEnvConfigMapType = "hub-manual-variables"
)

var (
	errConfigMapCache       = fmt.Errorf("failed to populate ConfigMap cache")
	errUnsupportedCmType    = fmt.Errorf("unsupported ConfigMap type")
	errNoHubNameLabel       = fmt.Errorf("ConfigMap does not have hub name label")
	errNoConfigMapTypeLabel = fmt.Errorf("ConfigMap does not have configmap type label")

	supportedConfigMapTypes = []string{EnvConfigMapType, ManualEnvConfigMapType, AutomationConfigMapType, VariablesSchemaMapType}
)

type ConfigMapStore interface {
	Run() error
	Stop()

	// Deprecated: use a type-specific GetAllHubs*ConfigMapData helper for deterministic lookups.
	GetAllHubsToDataMap() (map[string]map[string]string, error)
	// Deprecated: use GetEnvConfigMapDataByHubName, GetAutomationConfigMapDataByHubName, or GetVariablesSchemaConfigMapDataByHubName
	GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error)
	GetEnvConfigMapDataByHubName(hubName string) (map[string]string, bool, error)
	GetAutomationConfigMapDataByHubName(hubName string) (map[string]string, bool, error)
	GetVariablesSchemaConfigMapDataByHubName(hubName string) (map[string]string, bool, error)
	GetAllHubsEnvConfigMapData() (map[string]map[string]string, error)
	GetAllHubsAutomationConfigMapData() (map[string]map[string]string, error)
	GetAllHubsVariablesSchemaConfigMapData() (map[string]map[string]string, error)
}

type ConfigMapController struct {
	InformerFactory informers.SharedInformerFactory
	CmInformer      coreinformers.ConfigMapInformer
	namespace       string
	Logger          *zap.Logger
	stopCh          chan struct{}
}

func (cmc *ConfigMapController) Run() error {
	cmc.stopCh = make(chan struct{})

	cmc.InformerFactory.Start(cmc.stopCh)
	if !cache.WaitForCacheSync(cmc.stopCh, cmc.CmInformer.Informer().HasSynced) {
		return errConfigMapCache
	}
	return nil
}

func (cmc *ConfigMapController) Stop() {
	close(cmc.stopCh)
}

func NewConfigMapController(configMapTypes []string, namespace string, clientset kubernetes.Interface, logger *zap.Logger) (*ConfigMapController, error) {
	unsupportedTypes := lo.Filter(configMapTypes, func(item string, _ int) bool {
		return !lo.Contains(supportedConfigMapTypes, item)
	})
	if len(unsupportedTypes) > 0 {
		return nil, errUnsupportedCmType
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		time.Hour*24,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s in (%s)", ConfigMapTypeLabel, strings.Join(configMapTypes, ","))
		}),
	)

	cmInformer := informerFactory.Core().V1().ConfigMaps()

	if err := cmInformer.Informer().AddIndexers(map[string]cache.IndexFunc{
		ByHubAndType: func(obj interface{}) ([]string, error) {
			cm := obj.(*v1.ConfigMap)
			hubName, err := getHubName(cm)
			if err != nil {
				logger.Error("failed to get hub name for ConfigMap", zap.String("ConfigMap name", cm.Name))
				return nil, err
			}

			configMapType, err := getConfigMapType(cm)
			if err != nil {
				logger.Error("failed to get ConfigMap type", zap.String("ConfigMap name", cm.Name))
				return nil, err
			}

			return []string{getHubAndTypeKey(hubName, configMapType)}, nil
		},
		ByType: func(obj interface{}) ([]string, error) {
			cm := obj.(*v1.ConfigMap)
			configMapType, err := getConfigMapType(cm)
			if err != nil {
				logger.Error("failed to get ConfigMap type", zap.String("ConfigMap name", cm.Name))
				return nil, err
			}

			return []string{configMapType}, nil
		},
	}); err != nil {
		logger.Error("failed to add index", zap.Error(err))
		return nil, err
	}

	c := &ConfigMapController{
		namespace:       namespace,
		InformerFactory: informerFactory,
		CmInformer:      cmInformer,
		Logger:          logger,
	}

	return c, nil
}

func getHubName(configMap *v1.ConfigMap) (string, error) {
	if hubName, ok := configMap.Labels[LabelMdaiHubName]; ok {
		return hubName, nil
	}
	return "", errNoHubNameLabel
}

func getConfigMapType(configMap *v1.ConfigMap) (string, error) {
	if configMapType, ok := configMap.Labels[ConfigMapTypeLabel]; ok {
		return configMapType, nil
	}
	return "", errNoConfigMapTypeLabel
}

func getHubAndTypeKey(hubName string, configMapType string) string {
	// NUL cannot appear in Kubernetes label values
	return hubName + "\x00" + configMapType
}

func NewK8sClient(logger *zap.Logger) (kubernetes.Interface, error) {
	config, err := getKubeConfig(logger, os.UserHomeDir)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func NewK8sDynamicClient(logger *zap.Logger) (dynamic.Interface, error) {
	config, err := getKubeConfig(logger, os.UserHomeDir)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(config)
}

type HomeDirGetterFunc func() (string, error)

func getKubeConfig(logger *zap.Logger, homeDirGetterFunc HomeDirGetterFunc) (*rest.Config, error) {
	config, inClusterErr := rest.InClusterConfig()
	if inClusterErr != nil {
		// Try fetching config from the default file location
		homeDir, homeDirErr := homeDirGetterFunc()
		if homeDirErr != nil {
			logger.Error("Failed to load home directory for loading k8s config", zap.Error(homeDirErr))
			return nil, homeDirErr
		}

		fileConfig, kubeConfigFromFileErr := clientcmd.BuildConfigFromFlags("", homeDir+"/.kube/config")
		if kubeConfigFromFileErr != nil {
			logger.Error("Failed to build k8s config", zap.Error(kubeConfigFromFileErr))
			return nil, kubeConfigFromFileErr
		}
		config = fileConfig
	}
	return config, nil
}

// Deprecated: use a type-specific GetAllHubs*ConfigMapData helper for deterministic lookups.
// This method flattens multiple watched ConfigMap types into one value per hub and can be ambiguous.
// Still used in gateway
func (cmc *ConfigMapController) GetAllHubsToDataMap() (map[string]map[string]string, error) {
	hubMap := make(map[string]map[string]string)

	for _, obj := range cmc.CmInformer.Informer().GetIndexer().List() {
		cm, ok := obj.(*v1.ConfigMap)
		if !ok {
			cmc.Logger.Error("Failed to deserialize data to ConfigMap")
			continue
		}

		hubName, err := getHubName(cm)
		if err != nil {
			cmc.Logger.Error("Failed to get hub name for ConfigMap", zap.String("ConfigMap name", cm.Name), zap.Error(err))
			continue
		}
		hubMap[hubName] = cm.Data
	}
	return hubMap, nil
}

// getAllHubsToDataMapByType returns a hub->data map for a single ConfigMap type.
func (cmc *ConfigMapController) getAllHubsToDataMapByType(configMapType string) (map[string]map[string]string, error) {
	objs, err := cmc.CmInformer.Informer().GetIndexer().ByIndex(ByType, configMapType)
	if err != nil {
		return nil, fmt.Errorf("getting type by index: %w", err)
	}

	hubMap := make(map[string]map[string]string, len(objs))
	for _, obj := range objs {
		cm, ok := obj.(*v1.ConfigMap)
		if !ok {
			return nil, fmt.Errorf("failed to deserialize data to ConfigMap, type: %s", configMapType)
		}

		hubName, err := getHubName(cm)
		if err != nil {
			return nil, err
		}
		if _, exists := hubMap[hubName]; exists {
			return nil, fmt.Errorf("multiple ConfigMaps found for the same hub and type: %s, %s", hubName, configMapType)
		}

		hubMap[hubName] = cm.Data
	}

	return hubMap, nil
}

// GetAllHubsEnvConfigMapData returns variables config map data for all hubs.
func (cmc *ConfigMapController) GetAllHubsEnvConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(EnvConfigMapType)
}

// GetAllHubsAutomationConfigMapData returns automation config map data for all hubs.
func (cmc *ConfigMapController) GetAllHubsAutomationConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(AutomationConfigMapType)
}

// GetAllHubsVariablesSchemaConfigMapData returns variables schema config map data for all hubs.
func (cmc *ConfigMapController) GetAllHubsVariablesSchemaConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(VariablesSchemaMapType)
}

// Deprecated: use a type-specific Get*ConfigMapDataByHubName helper when the informer may watch multiple ConfigMap types per hub.
// GetConfigMapByHubName returns the only ConfigMap found for the given hub name.
// Still used in event hub
func (cmc *ConfigMapController) GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error) {
	var matchedConfigMaps []*v1.ConfigMap
	for _, obj := range cmc.CmInformer.Informer().GetIndexer().List() {
		cm, ok := obj.(*v1.ConfigMap)
		if !ok {
			cmc.Logger.Error("Failed to deserialize data to ConfigMap")
			continue
		}

		cmHubName, err := getHubName(cm)
		if err != nil {
			cmc.Logger.Error("Failed to get hub name for ConfigMap", zap.String("ConfigMap name", cm.Name), zap.Error(err))
			continue
		}
		if cmHubName != hubName {
			continue
		}

		matchedConfigMaps = append(matchedConfigMaps, cm)
	}

	if len(matchedConfigMaps) == 0 {
		return nil, fmt.Errorf("no ConfigMap found for hub: %s", hubName)
	}
	if len(matchedConfigMaps) > 1 {
		return nil, fmt.Errorf("multiple ConfigMaps found for the same hub: %s", hubName)
	}
	return matchedConfigMaps[0], nil
}

// getConfigMapDataByHubNameAndType returns config map data and whether the hub/type exists.
func (cmc *ConfigMapController) getConfigMapDataByHubNameAndType(hubName string, configMapType string) (map[string]string, bool, error) {
	indexer := cmc.CmInformer.Informer().GetIndexer()
	objs, err := indexer.ByIndex(ByHubAndType, getHubAndTypeKey(hubName, configMapType))
	if err != nil {
		return nil, false, fmt.Errorf("getting hub and type by index: %w", err)
	}
	if len(objs) == 0 {
		return nil, false, nil
	}
	if len(objs) > 1 {
		return nil, true, fmt.Errorf("multiple ConfigMaps found for the same hub and type: %s, %s", hubName, configMapType)
	}
	cm, ok := objs[0].(*v1.ConfigMap)
	if !ok {
		return nil, false, fmt.Errorf("failed to deserialize data to ConfigMap, hub name: %s, type: %s", hubName, configMapType)
	}

	return cm.Data, true, nil
}

// GetEnvConfigMapDataByHubName returns variables config map data for the given hub.
func (cmc *ConfigMapController) GetEnvConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, EnvConfigMapType)
}

// GetAutomationConfigMapDataByHubName returns automation config map data for the given hub.
func (cmc *ConfigMapController) GetAutomationConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, AutomationConfigMapType)
}

// GetVariablesSchemaConfigMapDataByHubName returns variables schema config map data for the given hub.
func (cmc *ConfigMapController) GetVariablesSchemaConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, VariablesSchemaMapType)
}
