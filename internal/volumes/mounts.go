package volumes

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const PodVolPerms os.FileMode = 0755

// defaultProjectedFileMode mirrors the Kubernetes default (0644) used for
// files produced by configMap/secret/downwardAPI projections when no
// defaultMode/mode is specified.
const defaultProjectedFileMode os.FileMode = 0644

// serviceAccountTokenMode is the mode kubelet uses for projected service
// account token files: tokens are credentials and must never be group/world
// readable.
const serviceAccountTokenMode os.FileMode = 0600

// Mount represents a universal mount point in a container.
// Note: This is a simplified version of the actual implementation
// and can be replaced by containerd's Mount type whenever (if) containerd
// is integrated into the project.
type Mount struct {
	Name          string
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// CreateContainerMounts creates the mounts for a container based on the pod spec.
func CreateContainerMounts(ctx context.Context, podVolRoot string, container corev1.Container, pod *corev1.Pod, serviceAccountToken string, configMaps map[string]*corev1.ConfigMap) ([]Mount, error) {
	mounts := []Mount{}
	secretGetter := secretGetterFromContext(ctx)
	if secretGetter == nil {
		// Lazy: the API client is only constructed when a pod actually
		// projects a secret.
		secretGetter = DefaultSecretGetter
	}

	for _, mountSpec := range container.VolumeMounts {
		podVolSpec := findPodVolumeSpec(pod, mountSpec.Name)
		if podVolSpec == nil {
			log.G(ctx).Debugf("Container volume mount %s not found in Pod spec", mountSpec.Name)
			continue
		}

		if mountSpec.SubPath != "" {
			if err := validateRelativePath(mountSpec.SubPath); err != nil {
				return nil, fmt.Errorf("invalid subPath for volume %s: %w", mountSpec.Name, err)
			}
		}

		// ContainerPath is always exactly the mountPath: with subPath the host
		// side of the mount points at volumeRoot/subPath, while the in-container
		// location stays unchanged.
		newMount := Mount{
			Name:          mountSpec.Name,
			ContainerPath: mountSpec.MountPath,
			ReadOnly:      mountSpec.ReadOnly,
		}

		// hostRoot is the on-host root directory of the volume. When subPath is
		// set the actual HostPath becomes hostRoot/subPath.
		var hostRoot string
		managedVolume := false
		// Iterate over the volume types we care about
		if podVolSpec.HostPath != nil {
			// create the host path if it doesn't exist
			err := os.MkdirAll(podVolSpec.HostPath.Path, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making hostPath for path %s: %w", podVolSpec.HostPath.Path, err)
			}
			hostRoot = podVolSpec.HostPath.Path
		} else if podVolSpec.EmptyDir != nil {
			// TODO: Currently ignores the SizeLimit
			hostRoot = filepath.Join(podVolRoot, mountSpec.Name)
			err := os.MkdirAll(hostRoot, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making emptyDir for path %s: %w", hostRoot, err)
			}
			managedVolume = true
		} else if podVolSpec.Projected != nil {
			hostRoot = filepath.Join(podVolRoot, mountSpec.Name)
			err := os.MkdirAll(hostRoot, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making projected for path %s: %w", hostRoot, err)
			}
			managedVolume = true
			if err := writeProjectedSources(ctx, hostRoot, pod, podVolSpec.Projected, serviceAccountToken, configMaps, secretGetter); err != nil {
				return nil, err
			}
		} else {
			continue
		}

		newMount.HostPath = hostRoot
		if mountSpec.SubPath != "" {
			newMount.HostPath = filepath.Join(hostRoot, filepath.FromSlash(mountSpec.SubPath))
			// Kubelet creates subPath directories inside managed volumes
			// (emptyDir/projected); hostPath subPaths must pre-exist.
			if managedVolume {
				if err := os.MkdirAll(newMount.HostPath, PodVolPerms); err != nil {
					return nil, fmt.Errorf("error making subPath %s for volume %s: %w", mountSpec.SubPath, mountSpec.Name, err)
				}
			}
		}
		mounts = append(mounts, newMount)
	}

	return mounts, nil
}

// writeProjectedSources materializes every projection source under volRoot.
func writeProjectedSources(ctx context.Context, volRoot string, pod *corev1.Pod, projected *corev1.ProjectedVolumeSource, serviceAccountToken string, configMaps map[string]*corev1.ConfigMap, secretGetter SecretGetter) error {
	for _, source := range projected.Sources {
		switch {
		case source.ServiceAccountToken != nil:
			err := writeProjectedFile(volRoot, source.ServiceAccountToken.Path, []byte(serviceAccountToken), serviceAccountTokenMode)
			if err != nil {
				return fmt.Errorf("error writing service account token: %w", err)
			}
		case source.ConfigMap != nil:
			configMap := configMaps[source.ConfigMap.Name]
			if configMap == nil {
				if isOptional(source.ConfigMap.Optional) {
					continue
				}
				return fmt.Errorf("config map %s not found", source.ConfigMap.Name)
			}
			if err := writeConfigMapProjection(volRoot, source.ConfigMap, configMap, projected.DefaultMode); err != nil {
				return err
			}
		case source.Secret != nil:
			secret, err := secretGetter(ctx, pod.Namespace, source.Secret.Name)
			if err != nil {
				if apierrors.IsNotFound(err) && isOptional(source.Secret.Optional) {
					continue
				}
				return fmt.Errorf("error getting secret %s: %w", source.Secret.Name, err)
			}
			if err := writeSecretProjection(volRoot, source.Secret, secret, projected.DefaultMode); err != nil {
				return err
			}
		case source.DownwardAPI != nil:
			// currently only namespace is supported
			for _, item := range source.DownwardAPI.Items {
				if item.FieldRef == nil || item.FieldRef.FieldPath != "metadata.namespace" {
					continue
				}
				err := writeProjectedFile(volRoot, item.Path, []byte(pod.Namespace), projectedFileMode(item.Mode, projected.DefaultMode))
				if err != nil {
					return fmt.Errorf("error writing downward API: %w", err)
				}
			}
		}
	}
	return nil
}

// writeConfigMapProjection writes the projected keys of a config map. With no
// explicit items, every key of Data and BinaryData is projected.
func writeConfigMapProjection(volRoot string, projection *corev1.ConfigMapProjection, configMap *corev1.ConfigMap, defaultMode *int32) error {
	if len(projection.Items) == 0 {
		for _, key := range sortedKeysString(configMap.Data) {
			if err := writeProjectedFile(volRoot, key, []byte(configMap.Data[key]), projectedFileMode(nil, defaultMode)); err != nil {
				return fmt.Errorf("error writing config map: %w", err)
			}
		}
		for _, key := range sortedKeysBinary(configMap.BinaryData) {
			if err := writeProjectedFile(volRoot, key, configMap.BinaryData[key], projectedFileMode(nil, defaultMode)); err != nil {
				return fmt.Errorf("error writing config map: %w", err)
			}
		}
		return nil
	}

	for _, keyToPath := range projection.Items {
		var value []byte
		if data, ok := configMap.Data[keyToPath.Key]; ok {
			value = []byte(data)
		} else if binaryData, ok := configMap.BinaryData[keyToPath.Key]; ok {
			value = binaryData
		} else if isOptional(projection.Optional) {
			continue
		} else {
			return fmt.Errorf("configmap %s references non-existent config key: %s", projection.Name, keyToPath.Key)
		}
		if err := writeProjectedFile(volRoot, keyToPath.Path, value, projectedFileMode(keyToPath.Mode, defaultMode)); err != nil {
			return fmt.Errorf("error writing config map: %w", err)
		}
	}
	return nil
}

// writeSecretProjection writes the projected keys of a secret. With no explicit
// items, every secret key is projected.
func writeSecretProjection(volRoot string, projection *corev1.SecretProjection, secret *corev1.Secret, defaultMode *int32) error {
	if len(projection.Items) == 0 {
		for _, key := range sortedKeysBinary(secret.Data) {
			if err := writeProjectedFile(volRoot, key, secret.Data[key], projectedFileMode(nil, defaultMode)); err != nil {
				return fmt.Errorf("error writing secret: %w", err)
			}
		}
		return nil
	}

	for _, keyToPath := range projection.Items {
		value, ok := secret.Data[keyToPath.Key]
		if !ok {
			if isOptional(projection.Optional) {
				continue
			}
			return fmt.Errorf("secret %s references non-existent secret key: %s", projection.Name, keyToPath.Key)
		}
		if err := writeProjectedFile(volRoot, keyToPath.Path, value, projectedFileMode(keyToPath.Mode, defaultMode)); err != nil {
			return fmt.Errorf("error writing secret: %w", err)
		}
	}
	return nil
}

// writeProjectedFile writes a single projected file. relPath is a relative
// path under the volume root and may contain subdirectories (e.g.
// "conf/app.yaml"); it must not be absolute or escape the root via "..".
func writeProjectedFile(volRoot, relPath string, data []byte, mode os.FileMode) error {
	if err := validateRelativePath(relPath); err != nil {
		return err
	}
	target := filepath.Join(volRoot, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(target), PodVolPerms); err != nil {
		return err
	}
	if err := os.WriteFile(target, data, mode); err != nil {
		return err
	}
	// Chmod explicitly so the umask cannot widen or narrow the requested mode.
	return os.Chmod(target, mode)
}

// projectedFileMode resolves the file mode for a projected file, preferring the
// per-item mode, then the volume defaultMode, then the Kubernetes default.
func projectedFileMode(itemMode, defaultMode *int32) os.FileMode {
	switch {
	case itemMode != nil:
		return os.FileMode(*itemMode) & os.ModePerm
	case defaultMode != nil:
		return os.FileMode(*defaultMode) & os.ModePerm
	default:
		return defaultProjectedFileMode
	}
}

// validateRelativePath mirrors the kubelet atomic writer path validation: the
// path must be relative, non-empty and must not contain ".." elements.
func validateRelativePath(targetPath string) error {
	if targetPath == "" {
		return fmt.Errorf("invalid path: must not be empty")
	}
	if path.IsAbs(targetPath) || filepath.IsAbs(targetPath) {
		return fmt.Errorf("invalid path: must be relative path: %s", targetPath)
	}
	for _, item := range strings.Split(filepath.ToSlash(targetPath), "/") {
		if item == ".." {
			return fmt.Errorf("invalid path: must not contain '..': %s", targetPath)
		}
	}
	return nil
}

func isOptional(optional *bool) bool {
	return optional != nil && *optional
}

func sortedKeysString(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func sortedKeysBinary(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// findPodVolumeSpec searches for a particular volume spec by name in the Pod spec
func findPodVolumeSpec(pod *corev1.Pod, name string) *corev1.VolumeSource {
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == name {
			return &volume.VolumeSource
		}
	}
	return nil
}
