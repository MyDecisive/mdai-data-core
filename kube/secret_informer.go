package kube

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
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

	GetSecretByName(name string) (*v1.Secret, error)
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
	unsupportedTypes := lo.Filter(secretTypes, func(item string, _ int) bool {
		return !lo.Contains(supportedSecretTypes, item)
	})
	if len(unsupportedTypes) > 0 {
		return nil, errors.New("unsupported ConfigMap type")
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		time.Hour*24,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = buildSecretLabelSelector(secretTypes)
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

func buildSecretLabelSelector(secretTypes []string) string {
	return fmt.Sprintf("%s in (%s)", SecretTypeLabel, strings.Join(secretTypes, ","))
}

// GetSecretByName returns the requested secret, if it exists.
func (sc *SecretController) GetSecretByName(name string) (*v1.Secret, error) {
	secret, err := sc.SecretLister.Secrets(sc.namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", sc.namespace, name, err)
	}
	return secret, nil
}
