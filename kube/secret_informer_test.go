package kube

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func newStartedSecretController(t *testing.T, watchedTypes []string, namespace string, secrets ...*corev1.Secret) (*SecretController, kubernetes.Interface) {
	t.Helper()

	objects := make([]runtime.Object, 0, len(secrets))
	for _, secret := range secrets {
		objects = append(objects, secret)
	}

	k8sClient := fake.NewClientset(objects...)
	controller, err := NewSecretController(watchedTypes, namespace, k8sClient, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, controller.Run())
	t.Cleanup(controller.Stop)

	return controller, k8sClient
}

func newTestSecret(name, namespace, hubName, secretType string, data map[string][]byte) *corev1.Secret { // nolint: unparam
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				SecretTypeLabel:  secretType,
				LabelMdaiHubName: hubName,
			},
		},
		Data: data,
	}
}

func TestSecretController_GetSecretByNameAndNamespace(t *testing.T) {
	t.Parallel()

	secret1 := newTestSecret(
		"mdaihub-first-octant-yolo",
		"first",
		"mdaihub-first",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-one": []byte("secret-one")},
	)
	secret2 := newTestSecret(
		"mdaihub-second-octant-yolo",
		"second",
		"mdaihub-second",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-two": []byte("secret-two")},
	)
	secret3 := newTestSecret(
		"mdaihub-second-octant-swag",
		"second",
		"mdaihub-third",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-three": []byte("secret-three")},
	)

	testCases := []struct {
		description     string
		namespace       string
		secrets         []*corev1.Secret
		expectedSecrets map[*corev1.Secret]bool
	}{
		{
			description: "only caching secrets for provided namespace",
			namespace:   "second",
			secrets:     []*corev1.Secret{secret1, secret2, secret3},
			expectedSecrets: map[*corev1.Secret]bool{
				secret1: false,
				secret2: true,
				secret3: true,
			},
		},
		{
			description: "namespace all caches for any namespace",
			namespace:   corev1.NamespaceAll,
			secrets:     []*corev1.Secret{secret1, secret2, secret3},
			expectedSecrets: map[*corev1.Secret]bool{
				secret1: true,
				secret2: true,
				secret3: true,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()

			controller, _ := newStartedSecretController(t, []string{OctantIntegrationArgoType}, tt.namespace, tt.secrets...)

			for secret, shouldFind := range tt.expectedSecrets {
				requireEventually(t, func(c *assert.CollectT) {
					cm, err := controller.GetSecretByNameAndNamespace(secret.Name, secret.Namespace)
					if shouldFind {
						assert.NoError(c, err)
						assert.NotNil(t, cm)
					} else {
						assert.Error(c, err)
						assert.Nil(t, cm)
					}
				})
			}
		})
	}
}

func TestNewSecretController_NonExistentSecretType(t *testing.T) {
	t.Parallel()

	controller, err := NewSecretController(
		[]string{"hub-nonexistent-secret-type"},
		"second",
		fake.NewClientset(),
		zap.NewNop(),
	)
	require.Nil(t, controller)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported Secret type")
}

func TestSecretController_CreateSecret(t *testing.T) {
	t.Parallel()

	secret1 := newTestSecret(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-one": []byte("secret-one")},
	)

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		controller, k8sClient := newStartedSecretController(t, []string{OctantIntegrationArgoType}, "bestNamespace")

		// verify no secrets exist yet
		secretList, err := k8sClient.CoreV1().Secrets("bestNamespace").List(t.Context(), metav1.ListOptions{})
		require.NoError(t, err)
		require.Empty(t, secretList.Items)

		require.NoError(t, controller.CreateSecret(t.Context(), "bestNamespace", secret1))

		// verify the single secret we created
		theSecret, err := k8sClient.CoreV1().Secrets("bestNamespace").Get(t.Context(), secret1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, secret1.Name, theSecret.Name)
	})
}

func TestSecretController_UpdateSecret(t *testing.T) {
	t.Parallel()

	secret1 := newTestSecret(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-one": []byte("secret-one")},
	)
	secret2 := newTestSecret(
		"mdaihub-first-octant-yolo",
		"bestNamespace",
		"mdaihub-first",
		OctantIntegrationArgoType,
		map[string][]byte{"secret-one": []byte("secret-one"), "secret-two": []byte("secret-two")},
	)

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		controller, k8sClient := newStartedSecretController(t, []string{OctantIntegrationArgoType}, "bestNamespace", secret1)

		// verify the secret exists with the single data key
		theSecret, err := k8sClient.CoreV1().Secrets("bestNamespace").Get(t.Context(), secret1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Len(t, theSecret.Data, 1)
		assert.Contains(t, theSecret.Data, "secret-one")

		require.NoError(t, controller.UpdateSecret(t.Context(), "bestNamespace", secret2))

		// verify the new secret key was added/updated
		theSecret, err = k8sClient.CoreV1().Secrets("bestNamespace").Get(t.Context(), secret1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Len(t, theSecret.Data, 2)
		assert.Contains(t, theSecret.Data, "secret-two")
	})
}
