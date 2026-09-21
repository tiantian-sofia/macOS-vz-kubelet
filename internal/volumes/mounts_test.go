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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateContainerMounts(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name                string
		container           corev1.Container
		pod                 *corev1.Pod
		serviceAccountToken string
		configMaps          map[string]*corev1.ConfigMap
		expectedMounts      []volumes.Mount
		expectError         bool
	}{
		{
			name: "HostPath volume",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "test-volume",
						MountPath: "/mnt/test",
					},
				},
			},
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "test-volume",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/tmp/hostpath",
								},
							},
						},
					},
				},
			},
			expectedMounts: []volumes.Mount{
				{
					Name:          "test-volume",
					HostPath:      "/tmp/hostpath",
					ContainerPath: "/mnt/test",
					ReadOnly:      false,
				},
			},
		},
		{
			name: "EmptyDir volume",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "emptydir-volume",
						MountPath: "/mnt/emptydir",
					},
				},
			},
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "emptydir-volume",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			expectedMounts: []volumes.Mount{
				{
					Name:          "emptydir-volume",
					HostPath:      filepath.Join(tempDir, "emptydir-volume"),
					ContainerPath: "/mnt/emptydir",
					ReadOnly:      false,
				},
			},
		},
		{
			name: "Projected volume with ServiceAccountToken",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "projected-volume",
						MountPath: "/mnt/projected",
					},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "projected-volume",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{
											ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
												Path: "token",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			serviceAccountToken: "test-token",
			expectedMounts: []volumes.Mount{
				{
					Name:          "projected-volume",
					HostPath:      filepath.Join(tempDir, "projected-volume"),
					ContainerPath: "/mnt/projected",
					ReadOnly:      false,
				},
			},
		},
		{
			name: "Projected volume with ConfigMap",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "configmap-volume",
						MountPath: "/mnt/configmap",
					},
				},
			},
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "configmap-volume",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{
											ConfigMap: &corev1.ConfigMapProjection{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: "test-configmap",
												},
												Items: []corev1.KeyToPath{
													{
														Key:  "config-key",
														Path: "config-path",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			configMaps: map[string]*corev1.ConfigMap{
				"test-configmap": {
					Data: map[string]string{
						"config-key": "config-value",
					},
				},
			},
			expectedMounts: []volumes.Mount{
				{
					Name:          "configmap-volume",
					HostPath:      filepath.Join(tempDir, "configmap-volume"),
					ContainerPath: "/mnt/configmap",
					ReadOnly:      false,
				},
			},
		},
		{
			name: "Projected volume with DownwardAPI (namespace)",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "downwardapi-volume",
						MountPath: "/mnt/downwardapi",
					},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "test-namespace",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "downwardapi-volume",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{
											DownwardAPI: &corev1.DownwardAPIProjection{
												Items: []corev1.DownwardAPIVolumeFile{
													{
														Path: "namespace",
														FieldRef: &corev1.ObjectFieldSelector{
															FieldPath: "metadata.namespace",
														},
														Mode: func(i int32) *int32 {
															return &i
														}(0644),
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedMounts: []volumes.Mount{
				{
					Name:          "downwardapi-volume",
					HostPath:      filepath.Join(tempDir, "downwardapi-volume"),
					ContainerPath: "/mnt/downwardapi",
					ReadOnly:      false,
				},
			},
		},
		{
			name: "Volume not found in Pod spec",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "non-existent-volume",
						MountPath: "/mnt/non-existent",
					},
				},
			},
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{},
				},
			},
			expectedMounts: []volumes.Mount{},
		},
		{
			name: "Unsupported volume type",
			container: corev1.Container{
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      "unsupported-volume",
						MountPath: "/mnt/unsupported",
					},
				},
			},
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "unsupported-volume",
							VolumeSource: corev1.VolumeSource{
								// This is an unsupported volume type for this function
								Secret: &corev1.SecretVolumeSource{
									SecretName: "my-secret",
								},
							},
						},
					},
				},
			},
			expectedMounts: []volumes.Mount{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mounts, err := volumes.CreateContainerMounts(context.Background(), tempDir, tt.container, tt.pod, tt.serviceAccountToken, tt.configMaps)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedMounts, mounts)
			}
		})
	}
}

// TestCreateContainerMountsProjectedFiles verifies what projected volumes
// actually materialize on disk: file contents, nested paths and permission
// bits.
func TestCreateContainerMountsProjectedFiles(t *testing.T) {
	configMapPod := func(cmName string, projected *corev1.ProjectedVolumeSource) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "test-namespace"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name:         "projected-volume",
					VolumeSource: corev1.VolumeSource{Projected: projected},
				}},
			},
		}
	}
	container := corev1.Container{
		VolumeMounts: []corev1.VolumeMount{{
			Name:      "projected-volume",
			MountPath: "/mnt/projected",
		}},
	}
	boolTrue := true
	boolFalse := false

	t.Run("token, full configMap and secret projection", func(t *testing.T) {
		tempDir := t.TempDir()
		pod := configMapPod("test-configmap", &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{
				{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
				{ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-configmap"},
				}},
				{Secret: &corev1.SecretProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-secret"},
				}},
			},
		})
		configMaps := map[string]*corev1.ConfigMap{
			"test-configmap": {
				Data: map[string]string{
					"plain":         "plain-value",
					"conf/app.yaml": "key: value\n",
				},
				BinaryData: map[string][]byte{
					"logo.png": {0x89, 0x50, 0x4e, 0x47},
				},
			},
		}

		mounts, err := volumes.CreateContainerMounts(context.Background(), tempDir, container, pod, "test-token", configMaps)
		require.NoError(t, err)
		require.Len(t, mounts, 1)
		volDir := filepath.Join(tempDir, "projected-volume")

		// service account token: content and 0644 permission bits (not 0755)
		tokenPath := filepath.Join(volDir, "token")
		content, err := os.ReadFile(tokenPath)
		require.NoError(t, err)
		assert.Equal(t, "test-token", string(content))
		info, err := os.Stat(tokenPath)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0644), info.Mode().Perm(), "token file must not be world-executable")

		// configMap projected without items: every key becomes a file,
		// including keys with slashes (nested directories created)
		plain, err := os.ReadFile(filepath.Join(volDir, "plain"))
		require.NoError(t, err)
		assert.Equal(t, "plain-value", string(plain))

		nested, err := os.ReadFile(filepath.Join(volDir, "conf", "app.yaml"))
		require.NoError(t, err)
		assert.Equal(t, "key: value\n", string(nested))
		nestedInfo, err := os.Stat(filepath.Join(volDir, "conf", "app.yaml"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0644), nestedInfo.Mode().Perm())

		binary, err := os.ReadFile(filepath.Join(volDir, "logo.png"))
		require.NoError(t, err)
		assert.Equal(t, []byte{0x89, 0x50, 0x4e, 0x47}, binary)
	})

	t.Run("items with per-file mode and defaultMode", func(t *testing.T) {
		tempDir := t.TempDir()
		pod := configMapPod("test-configmap", &corev1.ProjectedVolumeSource{
			DefaultMode: ptrInt32(0600),
			Sources: []corev1.VolumeProjection{{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-configmap"},
					Items: []corev1.KeyToPath{
						{Key: "config-key", Path: "config-path"},
						{Key: "exec-key", Path: "conf/exec.conf", Mode: ptrInt32(0755)},
					},
				},
			}},
		})
		configMaps := map[string]*corev1.ConfigMap{
			"test-configmap": {Data: map[string]string{
				"config-key": "config-value",
				"exec-key":   "exec-value",
			}},
		}

		_, err := volumes.CreateContainerMounts(context.Background(), tempDir, container, pod, "", configMaps)
		require.NoError(t, err)
		volDir := filepath.Join(tempDir, "projected-volume")

		content, err := os.ReadFile(filepath.Join(volDir, "config-path"))
		require.NoError(t, err)
		assert.Equal(t, "config-value", string(content))
		info, err := os.Stat(filepath.Join(volDir, "config-path"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0600), info.Mode().Perm(), "defaultMode must apply to items without explicit mode")

		execInfo, err := os.Stat(filepath.Join(volDir, "conf", "exec.conf"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0755), execInfo.Mode().Perm(), "explicit item mode must win")
	})

	t.Run("missing configMap tolerated only when optional", func(t *testing.T) {
		tempDir := t.TempDir()
		optionalPod := configMapPod("missing-configmap", &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "missing-configmap"},
					Optional:             &boolTrue,
				},
			}},
		})
		_, err := volumes.CreateContainerMounts(context.Background(), tempDir, container, optionalPod, "", map[string]*corev1.ConfigMap{})
		require.NoError(t, err)
		entries, err := os.ReadDir(filepath.Join(tempDir, "projected-volume"))
		require.NoError(t, err)
		assert.Empty(t, entries, "optional missing configMap must not create files")

		requiredPod := configMapPod("missing-configmap", &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "missing-configmap"},
					Optional:             &boolFalse,
				},
			}},
		})
		_, err = volumes.CreateContainerMounts(context.Background(), t.TempDir(), container, requiredPod, "", map[string]*corev1.ConfigMap{})
		require.Error(t, err)
	})

	t.Run("missing item key tolerated only when optional", func(t *testing.T) {
		projected := &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{
				ConfigMap: &corev1.ConfigMapProjection{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-configmap"},
					Items:                []corev1.KeyToPath{{Key: "missing-key", Path: "missing-path"}},
				},
			}},
		}
		configMaps := map[string]*corev1.ConfigMap{"test-configmap": {Data: map[string]string{"other": "x"}}}

		_, err := volumes.CreateContainerMounts(context.Background(), t.TempDir(), container, configMapPod("test-configmap", projected), "", configMaps)
		require.Error(t, err, "referencing a non-existent key without optional must fail")

		projected.Sources[0].ConfigMap.Optional = &boolTrue
		tempDir := t.TempDir()
		_, err = volumes.CreateContainerMounts(context.Background(), tempDir, container, configMapPod("test-configmap", projected), "", configMaps)
		require.NoError(t, err)
		entries, err := os.ReadDir(filepath.Join(tempDir, "projected-volume"))
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("rejects traversal paths", func(t *testing.T) {
		cases := []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "../escape"}},
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "/abs/path"}},
			{ConfigMap: &corev1.ConfigMapProjection{
				LocalObjectReference: corev1.LocalObjectReference{Name: "test-configmap"},
				Items:                []corev1.KeyToPath{{Key: "k", Path: "../../escape"}},
			}},
		}
		configMaps := map[string]*corev1.ConfigMap{"test-configmap": {Data: map[string]string{"k": "v"}}}
		for i, source := range cases {
			pod := configMapPod("test-configmap", &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{source}})
			_, err := volumes.CreateContainerMounts(context.Background(), t.TempDir(), container, pod, "tok", configMaps)
			require.Errorf(t, err, "case %d must be rejected", i)
		}
	})
}

// TestCreateContainerMountsSubPath verifies Kubernetes subPath semantics: the
// container sees mountPath unchanged while the host side points into the
// sub-directory of the volume (which is created when needed).
func TestCreateContainerMountsSubPath(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
	container := corev1.Container{
		VolumeMounts: []corev1.VolumeMount{{
			Name:      "data",
			MountPath: "/mnt/data",
			SubPath:   "cache/v1",
		}},
	}

	tempDir := t.TempDir()
	mounts, err := volumes.CreateContainerMounts(context.Background(), tempDir, container, pod, "", nil)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Equal(t, "/mnt/data", mounts[0].ContainerPath, "subPath must not be appended to mountPath")
	expectedHostPath := filepath.Join(tempDir, "data", "cache", "v1")
	assert.Equal(t, expectedHostPath, mounts[0].HostPath)
	info, err := os.Stat(expectedHostPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// a file pre-existing in the subPath directory is what the container sees
	// at mountPath
	require.NoError(t, os.WriteFile(filepath.Join(expectedHostPath, "marker"), []byte("present"), 0644))
	content, err := os.ReadFile(filepath.Join(expectedHostPath, "marker"))
	require.NoError(t, err)
	assert.Equal(t, "present", string(content))

	t.Run("subPath traversal rejected", func(t *testing.T) {
		badContainer := corev1.Container{VolumeMounts: []corev1.VolumeMount{{
			Name: "data", MountPath: "/mnt/data", SubPath: "../../etc",
		}}}
		_, err := volumes.CreateContainerMounts(context.Background(), t.TempDir(), badContainer, pod, "", nil)
		require.Error(t, err)
	})
}

func ptrInt32(v int32) *int32 { return &v }
