package kube

import (
	"context"
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
	CreateSecret(ctx context.Context, namespace string, secret *v1.Secret) error
	UpdateSecret(ctx context.Context, namespace string, secret *v1.Secret) error
}

type SecretController struct {
	informerFactory informers.SharedInformerFactory
	secretInformer  cache.SharedIndexInformer
	secretLister    corev1.SecretLister
	secretWriter    kubernetes.Interface
	namespace       string
	logger          *zap.Logger
	stopCh          chan struct{}
}

func (sc *SecretController) Run() error {
	sc.stopCh = make(chan struct{})

	sc.informerFactory.Start(sc.stopCh)
	if !cache.WaitForCacheSync(sc.stopCh, sc.secretInformer.HasSynced) {
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
		informerFactory: informerFactory,
		secretInformer:  informerFactory.Core().V1().Secrets().Informer(),
		secretLister:    informerFactory.Core().V1().Secrets().Lister(),
		logger:          logger,
		secretWriter:    clientset,
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
	secret, err := sc.secretLister.Secrets(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
	}
	return secret, nil
}

// CreateSecret creates the provided secret in the provided namespace.
func (sc *SecretController) CreateSecret(ctx context.Context, namespace string, secret *v1.Secret) error {
	if _, err := sc.secretWriter.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create secret %s: %w", secret.Name, err)
	}
	return nil
}

// UpdateSecret updates an existing secret with the provided secret in the provided namespace.
func (sc *SecretController) UpdateSecret(ctx context.Context, namespace string, secret *v1.Secret) error {
	if _, err := sc.secretWriter.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error while updating secret: %w", err)
	}
	return nil
}
