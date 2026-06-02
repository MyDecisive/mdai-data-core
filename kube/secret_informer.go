package kube

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	corev1 "k8s.io/client-go/listers/core/v1"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	SecretTypeLabel              = "mydecisive.ai/secret-type"
	OctantIntegrationDatadogType = "octant-integration-datadog"
	OctantIntegrationArgoType    = "octant-integration-argo"
)

var (
	supportedSecretTypes = []string{
		OctantIntegrationArgoType,
		OctantIntegrationDatadogType,
	}
)

type SecretStore interface {
	Run() error
	Stop()

	GetSecretByNameAndNamespace(name, namespace string) (*v1.Secret, error)
}

type SecretController struct {
	InformerFactory informers.SharedInformerFactory
	SecretInformer  cache.SharedIndexInformer
	SecretLister    corev1.SecretLister
	namespace       string
	Logger          *zap.Logger
	stopCh          chan struct{}
}

func (sc *SecretController) Run() error {
	sc.stopCh = make(chan struct{})

	sc.InformerFactory.Start(sc.stopCh)
	if !cache.WaitForCacheSync(sc.stopCh, sc.SecretInformer.HasSynced) {
		return errors.New("failed to populate Secret cache")
	}
	return nil
}

func (sc *SecretController) Stop() {
	close(sc.stopCh)
}

func NewSecretController(secretTypes []string, namespace string, clientset kubernetes.Interface, logger *zap.Logger) (*SecretController, error) {
	for _, secretType := range secretTypes {
		if !slices.Contains(supportedSecretTypes, secretType) {
			return nil, errors.New("unsupported Secret type")
		}
	}

	labelSelector, err := buildSecretLabelSelector(secretTypes)
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

	sc := &SecretController{
		namespace:       namespace,
		InformerFactory: informerFactory,
		SecretInformer:  informerFactory.Core().V1().Secrets().Informer(),
		SecretLister:    informerFactory.Core().V1().Secrets().Lister(),
		Logger:          logger,
	}

	return sc, nil
}

func buildSecretLabelSelector(secretTypes []string) (string, error) {
	req, err := labels.NewRequirement(SecretTypeLabel, selection.In, secretTypes)
	if err != nil {
		return "", err
	}
	return labels.NewSelector().Add(*req).String(), nil
}

// GetSecretByNameAndNamespace returns the requested secret, if it exists.
func (sc *SecretController) GetSecretByNameAndNamespace(name, namespace string) (*v1.Secret, error) {
	secret, err := sc.SecretLister.Secrets(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
	}
	return secret, nil
}
