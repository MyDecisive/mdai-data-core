package kube

import (
	"fmt"
	"github.com/samber/lo"
	"os"
	"strings"
	"time"

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
	ByHub                   = "IndexByHub"
	EnvConfigMapType        = "hub-variables"
	ManualEnvConfigMapType  = "hub-manual-variables"
	AutomationConfigMapType = "hub-automation"
	LabelMdaiHubName        = "mydecisive.ai/hub-name"
	ConfigMapTypeLabel      = "mydecisive.ai/configmap-type"
)

var (
	errConfigMapCache    = fmt.Errorf("failed to populate ConfigMap cache")
	errUnsupportedCmType = fmt.Errorf("unsupported ConfigMap type")
	errNoHubNamLabel     = fmt.Errorf("ConfigMap does not have hub name label")

	supportedConfigMapTypes = []string{EnvConfigMapType, ManualEnvConfigMapType, AutomationConfigMapType}
)

type ConfigMapStore interface {
	Run() error
	Stop()

	GetAllHubsToDataMap() (map[string]map[string]string, error)
	GetHubData(hubName string) ([]map[string]string, error)
	GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error)
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
		ByHub: func(obj interface{}) ([]string, error) {
			var hubNames []string
			hubName, err := getHubName(obj.(*v1.ConfigMap))
			if err != nil {
				logger.Error("failed to get hub name for ConfigMap", zap.String("ConfigMap name", obj.(*v1.ConfigMap).Name))
				return nil, err
			}
			hubNames = append(hubNames, hubName)
			return hubNames, nil
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
	return "", errNoHubNamLabel
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

func (cmc *ConfigMapController) GetAllHubsToDataMap() (map[string]map[string]string, error) {
	hubMap := make(map[string]map[string]string)

	indexer := cmc.CmInformer.Informer().GetIndexer()
	hubNames := indexer.ListIndexFuncValues(ByHub)
	for _, hubName := range hubNames {
		objs, err := indexer.ByIndex(ByHub, hubName)
		if err != nil {
			cmc.Logger.Error("Failed to get hub ConfigMaps", zap.String("Hub name", hubName), zap.Error(err))
			continue
		}
		for _, obj := range objs {
			cm, ok := obj.(*v1.ConfigMap)
			if !ok {
				cmc.Logger.Error("Failed to deserialize data to ConfigMap", zap.String("Hub name", hubName), zap.Error(err))
				continue
			}
			hubMap[hubName] = cm.Data
		}
	}
	return hubMap, nil
}

func (cmc *ConfigMapController) GetHubData(hubName string) ([]map[string]string, error) {
	indexer := cmc.CmInformer.Informer().GetIndexer()
	objs, err := indexer.ByIndex(ByHub, hubName)
	if err != nil {
		return nil, fmt.Errorf("getting hub by index: %w", err)
	}
	result := make([]map[string]string, 0, len(objs))
	for _, obj := range objs {
		cm, ok := obj.(*v1.ConfigMap)
		if !ok {
			cmc.Logger.Error("Failed to deserialize data to ConfigMap", zap.Error(err))
			continue
		}
		result = append(result, cm.Data)
	}
	return result, nil
}

// GetConfigMapByHubName returns the first ConfigMap found for the given hub name.
// This function assumes that the ConfigMaps of certain type filtered by label are unique per hub.
func (cmc *ConfigMapController) GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error) {
	indexer := cmc.CmInformer.Informer().GetIndexer()
	objs, err := indexer.ByIndex(ByHub, hubName)
	if err != nil {
		return nil, fmt.Errorf("getting hub by index: %w", err)
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("no ConfigMap found for hub: %s", hubName)
	}
	if len(objs) > 1 {
		return nil, fmt.Errorf("multiple ConfigMaps found for the same hub: %s", hubName)
	}
	cm, ok := objs[0].(*v1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("failed to deserialize data to ConfigMap, hub name: %s", hubName)
	}

	return cm, nil
}
