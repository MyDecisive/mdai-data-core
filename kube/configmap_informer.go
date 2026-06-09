package kube

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	ConfigMapTypeLabel             = "mydecisive.ai/configmap-type"
	OctantConnectionsConfigMapType = "octant-connections"
)

type ConfigMapStore interface {
	Run() error
	Stop()

	GetConfigmapByNameAndNamespace(name, namespace string) (*v1.ConfigMap, error)
	CreateConfigMap(ctx context.Context, namespace string, cm *v1.ConfigMap) error
	UpdateConfigMap(ctx context.Context, namespace string, cm *v1.ConfigMap) error
}

type ConfigMapController struct {
	informerFactory informers.SharedInformerFactory
	cmInformer      cache.SharedIndexInformer
	cmLister        corev1.ConfigMapLister
	cmWriter        kubernetes.Interface
	logger          *zap.Logger
	stopCh          chan struct{}
}

var _ ConfigMapStore = &ConfigMapController{}

func (cmc *ConfigMapController) Run() error {
	cmc.stopCh = make(chan struct{})

	cmc.informerFactory.Start(cmc.stopCh)
	if !cache.WaitForCacheSync(cmc.stopCh, cmc.cmInformer.HasSynced) {
		return errConfigMapCache
	}
	return nil
}

func (cmc *ConfigMapController) Stop() {
	close(cmc.stopCh)
}

func NewConfigMapController(configMapTypes []string, namespace string, clientset kubernetes.Interface, logger *zap.Logger) (*ConfigMapController, error) {
	for _, cmType := range configMapTypes {
		if !slices.Contains(supportedConfigMapTypes, cmType) {
			return nil, errUnsupportedCmType
		}
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

	c := &ConfigMapController{
		informerFactory: informerFactory,
		cmInformer:      informerFactory.Core().V1().ConfigMaps().Informer(),
		cmLister:        informerFactory.Core().V1().ConfigMaps().Lister(),
		logger:          logger,
		cmWriter:        clientset,
	}

	return c, nil
}

// GetConfigmapByNameAndNamespace returns the requested configmap, if it exists.
func (cmc *ConfigMapController) GetConfigmapByNameAndNamespace(name, namespace string) (*v1.ConfigMap, error) {
	cm, err := cmc.cmLister.ConfigMaps(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, name, err)
	}
	// return a deep copy so consumers can't directly modify the pointer to the object in cache.
	return cm.DeepCopy(), nil
}

// CreateConfigMap creates the provided configmap in the provided namespace.
func (cmc *ConfigMapController) CreateConfigMap(ctx context.Context, namespace string, cm *v1.ConfigMap) error {
	if _, err := cmc.cmWriter.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create configmap %s: %w", cm.Name, err)
	}
	return nil
}

// UpdateConfigMap updates an existing configmap with the provided configmap in the provided namespace.
func (cmc *ConfigMapController) UpdateConfigMap(ctx context.Context, namespace string, cm *v1.ConfigMap) error {
	if _, err := cmc.cmWriter.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error while updating configmap: %w", err)
	}
	return nil
}
