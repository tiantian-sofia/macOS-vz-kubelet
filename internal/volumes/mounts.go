package volumes

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	corev1 "k8s.io/api/core/v1"
)

const PodVolPerms os.FileMode = 0755

// projectedFilePerms is the default permission bits for files materialized
// inside a projected volume, matching ProjectedVolumeSourceDefaultMode (0644)
// in Kubernetes.
const projectedFilePerms os.FileMode = 0644

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
	for _, mountSpec := range container.VolumeMounts {
		podVolSpec := findPodVolumeSpec(pod, mountSpec.Name)
		if podVolSpec == nil {
			log.G(ctx).Debugf("Container volume mount %s not found in Pod spec", mountSpec.Name)
			continue
		}

		// Common fields to all mount types. ContainerPath is always the
		// mountPath: subPath selects a directory within the host-side volume
		// and does not change the path visible in the container.
		newMount := Mount{
			Name:          mountSpec.Name,
			ContainerPath: mountSpec.MountPath,
			ReadOnly:      mountSpec.ReadOnly,
		}
		// Iterate over the volume types we care about
		if podVolSpec.HostPath != nil {
			// create the host path if it doesn't exist
			err := os.MkdirAll(podVolSpec.HostPath.Path, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making hostPath for path %s: %w", podVolSpec.HostPath.Path, err)
			}
			newMount.HostPath = podVolSpec.HostPath.Path
		} else if podVolSpec.EmptyDir != nil {
			// TODO: Currently ignores the SizeLimit
			newMount.HostPath = filepath.Join(podVolRoot, mountSpec.Name)
			err := os.MkdirAll(newMount.HostPath, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making emptyDir for path %s: %w", newMount.HostPath, err)
			}
		} else if podVolSpec.Projected != nil {
			newMount.HostPath = filepath.Join(podVolRoot, mountSpec.Name)
			err := os.MkdirAll(newMount.HostPath, PodVolPerms)
			if err != nil {
				return nil, fmt.Errorf("error making projected for path %s: %w", newMount.HostPath, err)
			}
			err = populateProjectedVolume(ctx, newMount.HostPath, podVolSpec.Projected, pod, serviceAccountToken, configMaps)
			if err != nil {
				return nil, err
			}
		} else {
			continue
		}

		// subPath mounts a sub-directory of the host-side volume at
		// mountPath instead of the volume root.
		if mountSpec.SubPath != "" {
			hostPath, err := joinSubPath(newMount.HostPath, mountSpec.SubPath)
			if err != nil {
				return nil, fmt.Errorf("invalid subPath for volume %s: %w", mountSpec.Name, err)
			}
			if err := os.MkdirAll(hostPath, PodVolPerms); err != nil {
				return nil, fmt.Errorf("error making subPath %s for volume %s: %w", mountSpec.SubPath, mountSpec.Name, err)
			}
			newMount.HostPath = hostPath
		}

		mounts = append(mounts, newMount)
	}

	return mounts, nil
}

// populateProjectedVolume materializes the contents of a projected volume
// under hostPath.
func populateProjectedVolume(ctx context.Context, hostPath string, projected *corev1.ProjectedVolumeSource, pod *corev1.Pod, serviceAccountToken string, configMaps map[string]*corev1.ConfigMap) error {
	defaultMode := projectedFilePerms
	if projected.DefaultMode != nil {
		defaultMode = os.FileMode(*projected.DefaultMode)
	}

	for _, source := range projected.Sources {
		switch {
		case source.ServiceAccountToken != nil:
			if err := writeProjectedFile(hostPath, source.ServiceAccountToken.Path, []byte(serviceAccountToken), defaultMode); err != nil {
				return fmt.Errorf("error writing service account token: %w", err)
			}
		case source.ConfigMap != nil:
			files, err := configMapProjectionFiles(source.ConfigMap, configMaps[source.ConfigMap.Name], defaultMode)
			if err != nil {
				return err
			}
			for _, name := range sortedKeys(files) {
				file := files[name]
				if err := writeProjectedFile(hostPath, file.path, file.data, file.mode); err != nil {
					return fmt.Errorf("error writing config map %s: %w", source.ConfigMap.Name, err)
				}
			}
		case source.Secret != nil:
			// Secret payloads are not delivered to this layer by the pod
			// provider (see CreateContainerMounts inputs), so a secret cannot
			// be materialized here. Skip it loudly instead of pretending the
			// projection was applied.
			log.G(ctx).Warnf("secret projection %q is not supported; no secret data is available in this kubelet, files for this source will be missing", source.Secret.Name)
		case source.DownwardAPI != nil:
			// currently only namespace is supported
			for _, item := range source.DownwardAPI.Items {
				if item.FieldRef == nil || item.FieldRef.FieldPath != "metadata.namespace" {
					continue
				}
				mode := defaultMode
				if item.Mode != nil {
					mode = os.FileMode(*item.Mode)
				}
				if err := writeProjectedFile(hostPath, item.Path, []byte(pod.Namespace), mode); err != nil {
					return fmt.Errorf("error writing downward API: %w", err)
				}
			}
		}
	}
	return nil
}

type projectedFile struct {
	path string
	data []byte
	mode os.FileMode
}

// configMapProjectionFiles builds the set of files a configMap projection
// contributes, mirroring kubelet behavior: without items every key (including
// keys containing slashes) is projected at the same relative path; missing
// referenced config maps or keys are tolerated only when Optional is set.
func configMapProjectionFiles(projection *corev1.ConfigMapProjection, configMap *corev1.ConfigMap, defaultMode os.FileMode) (map[string]projectedFile, error) {
	optional := projection.Optional != nil && *projection.Optional
	if configMap == nil {
		if optional {
			return map[string]projectedFile{}, nil
		}
		return nil, fmt.Errorf("config map %s not found", projection.Name)
	}

	files := map[string]projectedFile{}
	add := func(key, path string, data []byte, itemMode *int32) {
		mode := defaultMode
		if itemMode != nil {
			mode = os.FileMode(*itemMode)
		}
		files[path] = projectedFile{path: path, data: data, mode: mode}
	}

	if len(projection.Items) == 0 {
		names := make([]string, 0, len(configMap.Data)+len(configMap.BinaryData))
		for name := range configMap.Data {
			names = append(names, name)
		}
		for name := range configMap.BinaryData {
			if _, ok := configMap.Data[name]; !ok {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			if data, ok := configMap.Data[name]; ok {
				add(name, name, []byte(data), nil)
			} else if data, ok := configMap.BinaryData[name]; ok {
				add(name, name, data, nil)
			}
		}
		return files, nil
	}

	for _, keyToPath := range projection.Items {
		if data, ok := configMap.Data[keyToPath.Key]; ok {
			add(keyToPath.Path, keyToPath.Path, []byte(data), keyToPath.Mode)
			continue
		}
		if data, ok := configMap.BinaryData[keyToPath.Key]; ok {
			add(keyToPath.Path, keyToPath.Path, data, keyToPath.Mode)
			continue
		}
		if !optional {
			return nil, fmt.Errorf("config map %s references non-existent config key: %s", projection.Name, keyToPath.Key)
		}
	}
	return files, nil
}

// writeProjectedFile writes a projected volume file under root, creating
// intermediate directories for relative paths containing slashes. Relative
// paths are validated the same way the Kubernetes API validates them: they
// must be non-empty, non-absolute and must not contain ".." elements.
func writeProjectedFile(root, relPath string, data []byte, mode os.FileMode) error {
	target, err := safeJoin(root, relPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), PodVolPerms); err != nil {
		return err
	}
	if err := os.WriteFile(target, data, mode.Perm()); err != nil {
		return err
	}
	// WriteFile applies the process umask, so set the requested bits
	// explicitly, like the kubelet atomic writer does.
	return os.Chmod(target, mode.Perm())
}

// joinSubPath appends a validated subPath to the host-side volume path.
func joinSubPath(volumePath, subPath string) (string, error) {
	return safeJoin(volumePath, subPath)
}

func safeJoin(root, relPath string) (string, error) {
	if relPath == "" || filepath.IsAbs(relPath) {
		return "", fmt.Errorf("path must be a non-empty relative path, got %q", relPath)
	}
	cleaned := filepath.Clean(relPath)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path must not contain '..', got %q", relPath)
	}
	return filepath.Join(root, cleaned), nil
}

func sortedKeys(files map[string]projectedFile) []string {
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
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
