package volumes_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/agoda-com/macOS-vz-kubelet/internal/volumes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func int32p(i int32) *int32 { return &i }
func boolp(b bool) *bool    { return &b }

func secretGetterStub(t *testing.T, secrets ...*corev1.Secret) volumes.SecretGetter {
	t.Helper()
	store := map[string]*corev1.Secret{}
	for _, secret := range secrets {
		store[secret.Namespace+"/"+secret.Name] = secret
	}
	return func(_ context.Context, namespace, name string) (*corev1.Secret, error) {
		if secret, ok := store[namespace+"/"+name]; ok {
			return secret, nil
		}
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
}

func readFileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}

func projectedPod(volumeName string, sources ...corev1.VolumeProjection) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test-namespace"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{Sources: sources},
					},
				},
			},
		},
	}
}

func projectedContainer(volumeName, mountPath, subPath string) corev1.Container {
	return corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{Name: volumeName, MountPath: mountPath, SubPath: subPath},
		},
	}
}

// TestProjectedServiceAccountTokenPermissions verifies the projected token is
// written on disk with 0600 regardless of the requested per-item mode.
func TestProjectedServiceAccountTokenPermissions(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "kube-api-access"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"},
	})

	mounts, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/var/run/secrets/kubernetes.io/serviceaccount", ""), pod, "super-secret-token", nil)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	tokenPath := filepath.Join(rootDir, volName, "token")
	content, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	assert.Equal(t, "super-secret-token", string(content))
	assert.Equal(t, os.FileMode(0600), readFileMode(t, tokenPath))
}

// TestProjectedConfigMapWithoutItems verifies that omitting items projects the
// whole config map (Data + BinaryData) with 0644 defaults.
func TestProjectedConfigMapWithoutItems(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-volume"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "settings"},
		},
	})
	configMaps := map[string]*corev1.ConfigMap{
		"settings": {
			Data:       map[string]string{"a.conf": "aaa", "b.properties": "bbb"},
			BinaryData: map[string][]byte{"logo.png": {0x89, 0x50}},
		},
	}

	mounts, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/etc/settings", ""), pod, "", configMaps)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	content, err := os.ReadFile(filepath.Join(rootDir, volName, "a.conf"))
	require.NoError(t, err)
	assert.Equal(t, "aaa", string(content))
	assert.Equal(t, os.FileMode(0644), readFileMode(t, filepath.Join(rootDir, volName, "a.conf")))

	content, err = os.ReadFile(filepath.Join(rootDir, volName, "b.properties"))
	require.NoError(t, err)
	assert.Equal(t, "bbb", string(content))

	content, err = os.ReadFile(filepath.Join(rootDir, volName, "logo.png"))
	require.NoError(t, err)
	assert.Equal(t, []byte{0x89, 0x50}, content)
}

// TestProjectedConfigMapNestedPath verifies that file paths containing
// relative subdirectories (e.g. conf/app.yaml) are created on disk.
func TestProjectedConfigMapNestedPath(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-nested"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "nested"},
			Items: []corev1.KeyToPath{
				{Key: "app", Path: "conf/app.yaml"},
			},
		},
	})
	configMaps := map[string]*corev1.ConfigMap{
		"nested": {Data: map[string]string{"app": "key: value\n"}},
	}

	mounts, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/nested", ""), pod, "", configMaps)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	content, err := os.ReadFile(filepath.Join(rootDir, volName, "conf", "app.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "key: value\n", string(content))
	assert.Equal(t, os.FileMode(0644), readFileMode(t, filepath.Join(rootDir, volName, "conf", "app.yaml")))
}

// TestProjectedConfigMapPathTraversal verifies absolute or ".." paths are
// rejected instead of escaping the volume root.
func TestProjectedConfigMapPathTraversal(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-evil"
	for _, evilPath := range []string{"../escape.yaml", "/abs/escape.yaml", "a/../../escape.yaml"} {
		t.Run(evilPath, func(t *testing.T) {
			pod := projectedPod(volName, corev1.VolumeProjection{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "evil"},
					Items:                []corev1.KeyToPath{{Key: "app", Path: evilPath}},
				},
			})
			configMaps := map[string]*corev1.ConfigMap{
				"evil": {Data: map[string]string{"app": "x"}},
			}
			_, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/evil", ""), pod, "", configMaps)
			require.Error(t, err)
		})
	}
}

// TestProjectedSecret verifies the secret projection is materialized on disk
// with the right contents and default 0644 mode, alongside a token and a
// config map in the same projected volume.
func TestProjectedSecret(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "mixed-projected"
	pod := projectedPod(volName,
		corev1.VolumeProjection{
			ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"},
		},
		corev1.VolumeProjection{
			ConfigMap: &corev1.ConfigMapProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"},
			},
		},
		corev1.VolumeProjection{
			Secret: &corev1.SecretProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
				Items: []corev1.KeyToPath{
					{Key: "username", Path: "username"},
					{Key: "password", Path: "password", Mode: int32p(0600)},
				},
			},
		},
	)
	configMaps := map[string]*corev1.ConfigMap{
		"cfg": {Data: map[string]string{"config": "value"}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test-namespace", Name: "creds"},
		Data:       map[string][]byte{"username": []byte("ci"), "password": []byte("hunter2")},
	}
	ctx := volumes.ContextWithSecretGetter(context.Background(), secretGetterStub(t, secret))

	mounts, err := volumes.CreateContainerMounts(ctx, rootDir, projectedContainer(volName, "/mnt/projected", ""), pod, "tok", configMaps)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	content, err := os.ReadFile(filepath.Join(rootDir, volName, "username"))
	require.NoError(t, err)
	assert.Equal(t, "ci", string(content))
	assert.Equal(t, os.FileMode(0644), readFileMode(t, filepath.Join(rootDir, volName, "username")))

	content, err = os.ReadFile(filepath.Join(rootDir, volName, "password"))
	require.NoError(t, err)
	assert.Equal(t, "hunter2", string(content))
	assert.Equal(t, os.FileMode(0600), readFileMode(t, filepath.Join(rootDir, volName, "password")))

	content, err = os.ReadFile(filepath.Join(rootDir, volName, "config"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(content))

	content, err = os.ReadFile(filepath.Join(rootDir, volName, "token"))
	require.NoError(t, err)
	assert.Equal(t, "tok", string(content))
	assert.Equal(t, os.FileMode(0600), readFileMode(t, filepath.Join(rootDir, volName, "token")))
}

// TestProjectedSecretWithoutItems verifies all secret keys are projected when
// items is omitted.
func TestProjectedSecretWithoutItems(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "secret-full"
	pod := projectedPod(volName, corev1.VolumeProjection{
		Secret: &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "all"},
		},
	})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test-namespace", Name: "all"},
		Data:       map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")},
	}
	ctx := volumes.ContextWithSecretGetter(context.Background(), secretGetterStub(t, secret))

	_, err := volumes.CreateContainerMounts(ctx, rootDir, projectedContainer(volName, "/mnt/secret", ""), pod, "", nil)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(rootDir, volName, "k1"))
	require.NoError(t, err)
	assert.Equal(t, "v1", string(content))
	content, err = os.ReadFile(filepath.Join(rootDir, volName, "k2"))
	require.NoError(t, err)
	assert.Equal(t, "v2", string(content))
}

// TestProjectedSecretOptionalMissing verifies an optional missing secret does
// not fail pod creation and projects nothing.
func TestProjectedSecretOptionalMissing(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "secret-optional"
	pod := projectedPod(volName, corev1.VolumeProjection{
		Secret: &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ghost"},
			Optional:             boolp(true),
		},
	})
	ctx := volumes.ContextWithSecretGetter(context.Background(), secretGetterStub(t))

	mounts, err := volumes.CreateContainerMounts(ctx, rootDir, projectedContainer(volName, "/mnt/optional", ""), pod, "", nil)
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	entries, err := os.ReadDir(filepath.Join(rootDir, volName))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// TestProjectedSecretRequiredMissing verifies a required missing secret still
// fails pod creation.
func TestProjectedSecretRequiredMissing(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "secret-required"
	pod := projectedPod(volName, corev1.VolumeProjection{
		Secret: &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "ghost"},
		},
	})
	ctx := volumes.ContextWithSecretGetter(context.Background(), secretGetterStub(t))

	_, err := volumes.CreateContainerMounts(ctx, rootDir, projectedContainer(volName, "/mnt/required", ""), pod, "", nil)
	require.Error(t, err)
}

// TestProjectedConfigMapOptionalMissing verifies optional semantics: a missing
// config map is skipped when optional is set, and a missing key is skipped too.
func TestProjectedConfigMapOptionalMissing(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-optional"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "maybe"},
			Optional:             boolp(true),
		},
	})

	mounts, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/optional", ""), pod, "", map[string]*corev1.ConfigMap{})
	require.NoError(t, err)
	require.Len(t, mounts, 1)

	entries, err := os.ReadDir(filepath.Join(rootDir, volName))
	require.NoError(t, err)
	assert.Empty(t, entries)

	// optional config map referencing a missing key: skipped, present key still written
	pod2 := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "maybe2"},
			Optional:             boolp(true),
			Items: []corev1.KeyToPath{
				{Key: "present", Path: "present"},
				{Key: "absent", Path: "absent"},
			},
		},
	})
	configMaps := map[string]*corev1.ConfigMap{
		"maybe2": {Data: map[string]string{"present": "yes"}},
	}
	mounts, err = volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/optional2", ""), pod2, "", configMaps)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	content, err := os.ReadFile(filepath.Join(rootDir, volName, "present"))
	require.NoError(t, err)
	assert.Equal(t, "yes", string(content))
	_, err = os.Stat(filepath.Join(rootDir, volName, "absent"))
	assert.True(t, os.IsNotExist(err))
}

// TestProjectedConfigMapRequiredMissing verifies a missing required config map
// fails pod creation.
func TestProjectedConfigMapRequiredMissing(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-required"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "nope"},
		},
	})
	_, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/cm", ""), pod, "", map[string]*corev1.ConfigMap{})
	require.Error(t, err)

	// present config map, missing required key
	pod2 := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "cm2"},
			Items:                []corev1.KeyToPath{{Key: "missing", Path: "missing"}},
		},
	})
	_, err = volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/cm2", ""), pod2, "",
		map[string]*corev1.ConfigMap{"cm2": {Data: map[string]string{}}})
	require.Error(t, err)
}

// TestProjectedDefaultMode verifies the projected volume defaultMode is
// applied to projected files that do not specify a per-item mode.
func TestProjectedDefaultMode(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "cm-default-mode"
	pod := projectedPod(volName, corev1.VolumeProjection{
		ConfigMap: &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: "mode-cm"},
			Items: []corev1.KeyToPath{
				{Key: "a", Path: "a"},
				{Key: "b", Path: "b", Mode: int32p(0600)},
			},
		},
	})
	pod.Spec.Volumes[0].Projected.DefaultMode = int32p(0440)
	configMaps := map[string]*corev1.ConfigMap{
		"mode-cm": {Data: map[string]string{"a": "1", "b": "2"}},
	}

	_, err := volumes.CreateContainerMounts(context.Background(), rootDir, projectedContainer(volName, "/mnt/mode", ""), pod, "", configMaps)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0440), readFileMode(t, filepath.Join(rootDir, volName, "a")))
	assert.Equal(t, os.FileMode(0600), readFileMode(t, filepath.Join(rootDir, volName, "b")))
}

// TestEmptyDirSubPath verifies that subPath selects a host-side subdirectory
// while ContainerPath remains exactly MountPath, and that the subdirectory is
// created inside the emptyDir.
func TestEmptyDirSubPath(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "workdir"
	container := corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{Name: volName, MountPath: "/workspace", SubPath: "repo/src"},
		},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: volName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}

	mounts, err := volumes.CreateContainerMounts(context.Background(), rootDir, container, pod, "", nil)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Equal(t, "/workspace", mounts[0].ContainerPath)
	assert.Equal(t, filepath.Join(rootDir, volName, "repo", "src"), mounts[0].HostPath)
	info, err := os.Stat(mounts[0].HostPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// data written by another container at the volume root stays outside the
	// subPath bind and remains visible via a second, subPath-less mount.
	other := corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{Name: volName, MountPath: "/shared"},
		},
	}
	mounts2, err := volumes.CreateContainerMounts(context.Background(), rootDir, other, pod, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "/shared", mounts2[0].ContainerPath)
	assert.Equal(t, filepath.Join(rootDir, volName), mounts2[0].HostPath)
}

// TestSubPathTraversal verifies malicious subPath values are rejected.
func TestSubPathTraversal(t *testing.T) {
	rootDir := t.TempDir()
	const volName = "ed"
	container := corev1.Container{
		VolumeMounts: []corev1.VolumeMount{
			{Name: volName, MountPath: "/data", SubPath: "../../etc"},
		},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: volName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
	_, err := volumes.CreateContainerMounts(context.Background(), rootDir, container, pod, "", nil)
	require.Error(t, err)
}
