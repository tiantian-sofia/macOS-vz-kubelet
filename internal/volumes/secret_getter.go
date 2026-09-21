package volumes

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// SecretGetter fetches a secret by namespace/name.
type SecretGetter func(ctx context.Context, namespace, name string) (*corev1.Secret, error)

type secretGetterContextKey struct{}

// ContextWithSecretGetter returns a context carrying a custom secret getter.
// It is primarily an injection seam for tests; production code falls back to
// a lazy Kubernetes API client.
func ContextWithSecretGetter(ctx context.Context, getter SecretGetter) context.Context {
	return context.WithValue(ctx, secretGetterContextKey{}, getter)
}

func secretGetterFromContext(ctx context.Context) SecretGetter {
	getter, _ := ctx.Value(secretGetterContextKey{}).(SecretGetter)
	return getter
}

var (
	defaultSecretGetterMu sync.Mutex
	defaultSecretGetter   SecretGetter
)

// DefaultSecretGetter returns a SecretGetter backed by the in-cluster
// Kubernetes API, falling back to the local kubeconfig for development.
func DefaultSecretGetter(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	getter, err := getDefaultSecretGetter()
	if err != nil {
		return nil, err
	}
	return getter(ctx, namespace, name)
}

func getDefaultSecretGetter() (SecretGetter, error) {
	defaultSecretGetterMu.Lock()
	defer defaultSecretGetterMu.Unlock()
	if defaultSecretGetter != nil {
		return defaultSecretGetter, nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	defaultSecretGetter = func(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
		return clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	return defaultSecretGetter, nil
}
