package kube

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"k8s.io/client-go/dynamic"

	"go.uber.org/zap"
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
	// Deprecated: migrate to VariablesSchemaMapType.
	ManualEnvConfigMapType = "hub-manual-variables"
)

var (
	errConfigMapCache       = errors.New("failed to populate ConfigMap cache")
	errUnsupportedCmType    = errors.New("unsupported ConfigMap type")
	errNoHubNameLabel       = errors.New("ConfigMap does not have hub name label")
	errNoConfigMapTypeLabel = errors.New("ConfigMap does not have configmap type label")

	supportedConfigMapTypes = []string{
		EnvConfigMapType,
		ManualEnvConfigMapType,
		AutomationConfigMapType,
		VariablesSchemaMapType,
		OctantConnectionsConfigMapType,
	}
)

type HubConfigMapStore interface {
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

type HubConfigMapController struct {
	InformerFactory informers.SharedInformerFactory
	CmInformer      coreinformers.ConfigMapInformer
	namespace       string
	Logger          *zap.Logger
	lifecycle       informerLifecycle
}

var _ HubConfigMapStore = &HubConfigMapController{}

// Run starts the informer and waits for its cache to sync.
func (cmc *HubConfigMapController) Run() error {
	stopCh := cmc.lifecycle.start()

	cmc.InformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, cmc.CmInformer.Informer().HasSynced) {
		return errConfigMapCache
	}
	return nil
}

// Stop stops the informer. It is safe to call more than once, and before Run.
func (cmc *HubConfigMapController) Stop() {
	cmc.lifecycle.stop()
}

func NewHubConfigMapController(configMapTypes []string, namespace string, clientset kubernetes.Interface, logger *zap.Logger) (*HubConfigMapController, error) {
	unsupportedTypes := lo.Filter(configMapTypes, func(item string, _ int) bool {
		return !lo.Contains(supportedConfigMapTypes, item)
	})
	if len(unsupportedTypes) > 0 {
		return nil, errUnsupportedCmType
	}

	labelSelector, err := buildConfigmapLabelSelector(configMapTypes)
	if err != nil {
		return nil, err
	}
	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		time.Hour*24,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector
		}),
	)

	cmInformer := informerFactory.Core().V1().ConfigMaps()
	if err := cmInformer.Informer().AddIndexers(map[string]cache.IndexFunc{
		ByHubAndType: func(obj interface{}) ([]string, error) {
			return byHubAndTypeIndex(logger, obj)
		},
		ByType: func(obj interface{}) ([]string, error) {
			return byTypeIndex(logger, obj)
		},
	}); err != nil {
		logger.Error("failed to add index", zap.Error(err))
		return nil, err
	}

	c := &HubConfigMapController{
		namespace:       namespace,
		InformerFactory: informerFactory,
		CmInformer:      cmInformer,
		Logger:          logger,
	}

	return c, nil
}

func buildConfigmapLabelSelector(configMapTypes []string) (string, error) {
	req, err := labels.NewRequirement(ConfigMapTypeLabel, selection.In, configMapTypes)
	if err != nil {
		return "", err
	}
	return labels.NewSelector().Add(*req).String(), nil
}

// informerLifecycle holds an informer's stop channel. It makes Stop safe to call more than
// once or before Run, and Run safe to call again while running.
type informerLifecycle struct {
	mu     sync.Mutex
	stopCh chan struct{}
}

// start returns the stop channel to pass to the informer, creating it if not running.
func (l *informerLifecycle) start() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopCh == nil {
		l.stopCh = make(chan struct{})
	}
	return l.stopCh
}

// stop closes the stop channel if running; otherwise it does nothing.
func (l *informerLifecycle) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopCh != nil {
		close(l.stopCh)
		l.stopCh = nil
	}
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

// byHubAndTypeIndex must never return an error: client-go panics the informer on
// any IndexFunc error, and ConfigMaps missing the hub-name label legitimately
// reach here (the watch filter requires only the type label).
func byHubAndTypeIndex(logger *zap.Logger, obj interface{}) ([]string, error) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		return nil, nil
	}
	hubName, err := getHubName(cm)
	if err != nil {
		logger.Warn("skipping ConfigMap without hub name label from hub/type index", zap.String("ConfigMap name", cm.Name))
		return nil, nil
	}
	configMapType, err := getConfigMapType(cm)
	if err != nil {
		logger.Warn("skipping ConfigMap without type label from hub/type index", zap.String("ConfigMap name", cm.Name))
		return nil, nil
	}
	return []string{getHubAndTypeKey(hubName, configMapType)}, nil
}

// byTypeIndex must never return an error, for the same reason as byHubAndTypeIndex.
// The watch filter requires the type label, so a ConfigMap without it should not reach
// here; if one does, it is skipped rather than failing the informer.
func byTypeIndex(logger *zap.Logger, obj interface{}) ([]string, error) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		return nil, nil
	}
	configMapType, err := getConfigMapType(cm)
	if err != nil {
		logger.Warn("skipping ConfigMap without type label from type index", zap.String("ConfigMap name", cm.Name))
		return nil, nil
	}
	return []string{configMapType}, nil
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
func (cmc *HubConfigMapController) GetAllHubsToDataMap() (map[string]map[string]string, error) {
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
		// copy so callers can't modify the informer cache through the returned map.
		hubMap[hubName] = maps.Clone(cm.Data)
	}
	return hubMap, nil
}

// getAllHubsToDataMapByType returns a hub->data map for a single ConfigMap type.
func (cmc *HubConfigMapController) getAllHubsToDataMapByType(configMapType string) (map[string]map[string]string, error) {
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

		// copy so callers can't modify the informer cache through the returned map.
		hubMap[hubName] = maps.Clone(cm.Data)
	}

	return hubMap, nil
}

// GetAllHubsEnvConfigMapData returns variables config map data for all hubs.
func (cmc *HubConfigMapController) GetAllHubsEnvConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(EnvConfigMapType)
}

// GetAllHubsAutomationConfigMapData returns automation config map data for all hubs.
func (cmc *HubConfigMapController) GetAllHubsAutomationConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(AutomationConfigMapType)
}

// GetAllHubsVariablesSchemaConfigMapData returns variables schema config map data for all hubs.
func (cmc *HubConfigMapController) GetAllHubsVariablesSchemaConfigMapData() (map[string]map[string]string, error) {
	return cmc.getAllHubsToDataMapByType(VariablesSchemaMapType)
}

// Deprecated: use a type-specific Get*ConfigMapDataByHubName helper when the informer may watch multiple ConfigMap types per hub.
// GetConfigMapByHubName returns the only ConfigMap found for the given hub name.
// Still used in event hub
func (cmc *HubConfigMapController) GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error) {
	var matchedConfigMap *v1.ConfigMap
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

		if matchedConfigMap != nil {
			return nil, fmt.Errorf("multiple ConfigMaps found for the same hub: %s", hubName)
		}

		matchedConfigMap = cm
	}

	if matchedConfigMap == nil {
		return nil, fmt.Errorf("no ConfigMap found for hub: %s", hubName)
	}

	// return a deep copy so consumers can't directly modify the pointer to the object in cache.
	return matchedConfigMap.DeepCopy(), nil
}

// getConfigMapDataByHubNameAndType returns config map data and whether the hub/type exists.
func (cmc *HubConfigMapController) getConfigMapDataByHubNameAndType(hubName string, configMapType string) (map[string]string, bool, error) {
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

	// copy so callers can't modify the informer cache through the returned map.
	return maps.Clone(cm.Data), true, nil
}

// GetEnvConfigMapDataByHubName returns variables config map data for the given hub.
func (cmc *HubConfigMapController) GetEnvConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, EnvConfigMapType)
}

// GetAutomationConfigMapDataByHubName returns automation config map data for the given hub.
func (cmc *HubConfigMapController) GetAutomationConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, AutomationConfigMapType)
}

// GetVariablesSchemaConfigMapDataByHubName returns variables schema config map data for the given hub.
func (cmc *HubConfigMapController) GetVariablesSchemaConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return cmc.getConfigMapDataByHubNameAndType(hubName, VariablesSchemaMapType)
}
