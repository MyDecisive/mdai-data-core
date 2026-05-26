package kube

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	ConfigMapTypeLabel             = "mydecisive.ai/configmap-type"
	OctantConnectionsConfigMapType = "octant-connections"
)

type ConfigMapStore interface {
	Run() error
	Stop()

	GetConfigmapByName(name string) (*v1.ConfigMap, error)
}

type ConfigMapController struct {
	InformerFactory informers.SharedInformerFactory
	CmInformer      coreinformers.ConfigMapInformer
	namespace       string
	Logger          *zap.Logger
	stopCh          chan struct{}
}

var _ ConfigMapStore = &ConfigMapController{}

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
			opts.LabelSelector = buildConfigmapLabelSelector(configMapTypes)
		}),
	)

	cmInformer := informerFactory.Core().V1().ConfigMaps()

	c := &ConfigMapController{
		namespace:       namespace,
		InformerFactory: informerFactory,
		CmInformer:      cmInformer,
		Logger:          logger,
	}

	return c, nil
}

// GetConfigmapByName returns the requested configmap, if it exists.
func (cmc *ConfigMapController) GetConfigmapByName(name string) (*v1.ConfigMap, error) {
	cm, err := cmc.CmInformer.Lister().ConfigMaps(cmc.namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", cmc.namespace, name, err)
	}
	return cm, nil
}
