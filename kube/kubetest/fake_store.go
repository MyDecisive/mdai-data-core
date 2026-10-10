package kubetest

import (
	"fmt"
	"maps"
	"sync"

	"github.com/mydecisive/mdai-data-core/kube"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ kube.HubConfigMapStore = (*FakeConfigMapStore)(nil)

// FakeConfigMapStore is a threadsafe in-memory implementation of kube.ConfigMapStore.
// It avoids client-go informers and lets tests seed & assert data deterministically.
// Like the real controller, it copies maps and ConfigMaps on the way in and out, so neither
// the test nor the code under test can change the store's state through a shared reference.
type FakeConfigMapStore struct {
	mu sync.RWMutex

	// hub -> configmaps
	byHub  map[string][]*v1.ConfigMap
	allCMs map[string]*v1.ConfigMap

	// Optional error injection points
	RunErr                       error
	GetAllHubsToDataMapErr       error
	GetAllHubsToDataMapByTypeErr error
	GetConfigMapByHubNameErr     error
	GetConfigMapByHubAndTypeErr  error
	GetConfigMapByNameErr        error

	// Lifecycle flags
	running bool
	stopped bool
}

// NewFakeConfigMapStore creates an empty fake store.
func NewFakeConfigMapStore() *FakeConfigMapStore {
	return &FakeConfigMapStore{
		byHub:  make(map[string][]*v1.ConfigMap),
		allCMs: make(map[string]*v1.ConfigMap),
	}
}

// SeedConfigMap adds (or appends) a ConfigMap for the given hub.
func (f *FakeConfigMapStore) SeedConfigMap(hubName, cmName, cmType string, data map[string]string) *FakeConfigMapStore {
	f.mu.Lock()
	defer f.mu.Unlock()

	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: cmName,
			Labels: map[string]string{
				kube.LabelMdaiHubName:   hubName,
				kube.ConfigMapTypeLabel: cmType,
			},
		},
		Data: maps.Clone(data),
	}
	f.byHub[hubName] = append(f.byHub[hubName], cm)
	f.allCMs[cmName] = cm
	return f
}

// Reset clears all state.
func (f *FakeConfigMapStore) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byHub = make(map[string][]*v1.ConfigMap)
	f.allCMs = make(map[string]*v1.ConfigMap)
	f.running = false
	f.stopped = false
	f.RunErr = nil
	f.GetAllHubsToDataMapErr = nil
	f.GetAllHubsToDataMapByTypeErr = nil
	f.GetConfigMapByHubNameErr = nil
	f.GetConfigMapByHubAndTypeErr = nil
	f.GetConfigMapByNameErr = nil
}

func (f *FakeConfigMapStore) Run() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.RunErr != nil {
		return f.RunErr
	}
	f.running = true
	return nil
}

func (f *FakeConfigMapStore) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = true
}

// GetAllHubsToDataMap returns a map[hub]data.
func (f *FakeConfigMapStore) GetAllHubsToDataMap() (map[string]map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetAllHubsToDataMapErr != nil {
		return nil, f.GetAllHubsToDataMapErr
	}

	out := make(map[string]map[string]string, len(f.byHub))
	for hub, cms := range f.byHub {
		if len(cms) == 0 {
			continue
		}
		// keep parity with controller: last wins
		out[hub] = maps.Clone(cms[len(cms)-1].Data)
	}
	return out, nil
}

func (f *FakeConfigMapStore) getAllHubsToDataMapByType(configMapType string) (map[string]map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetAllHubsToDataMapByTypeErr != nil {
		return nil, f.GetAllHubsToDataMapByTypeErr
	}

	out := make(map[string]map[string]string)
	for hub, cms := range f.byHub {
		for _, cm := range cms {
			if cm.Labels[kube.ConfigMapTypeLabel] != configMapType {
				continue
			}
			if _, exists := out[hub]; exists {
				return nil, fmt.Errorf("multiple ConfigMaps found for the same hub and type: %s, %s", hub, configMapType)
			}
			out[hub] = maps.Clone(cm.Data)
		}
	}
	return out, nil
}

func (f *FakeConfigMapStore) GetAllHubsEnvConfigMapData() (map[string]map[string]string, error) {
	return f.getAllHubsToDataMapByType(kube.EnvConfigMapType)
}

func (f *FakeConfigMapStore) GetAllHubsAutomationConfigMapData() (map[string]map[string]string, error) {
	return f.getAllHubsToDataMapByType(kube.AutomationConfigMapType)
}

func (f *FakeConfigMapStore) GetAllHubsVariablesSchemaConfigMapData() (map[string]map[string]string, error) {
	return f.getAllHubsToDataMapByType(kube.VariablesSchemaMapType)
}

// GetConfigmapByNameAndNamespace returns a copy of the named ConfigMap. Like the real controller,
// it returns an error satisfying apierrors.IsNotFound when there is none. The namespace is only
// used in the error message: seeded ConfigMaps have no namespace, so lookups match by name.
func (f *FakeConfigMapStore) GetConfigmapByNameAndNamespace(name, namespace string) (*v1.ConfigMap, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetConfigMapByNameErr != nil {
		return nil, f.GetConfigMapByNameErr
	}

	cm, ok := f.allCMs[name]
	if !ok {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, name, apierrors.NewNotFound(v1.Resource("configmap"), name))
	}
	return cm.DeepCopy(), nil
}

func (f *FakeConfigMapStore) GetConfigMapByHubName(hubName string) (*v1.ConfigMap, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetConfigMapByHubNameErr != nil {
		return nil, f.GetConfigMapByHubNameErr
	}

	configMaps := f.byHub[hubName]
	switch len(configMaps) {
	case 0:
		return nil, fmt.Errorf("no ConfigMap found for hub: %s", hubName)
	case 1:
		return configMaps[0].DeepCopy(), nil
	default:
		names := make([]string, len(configMaps))
		for i, cm := range configMaps {
			names[i] = cm.Name
		}
		return nil, fmt.Errorf("multiple ConfigMaps %v found for the same hub: %s", names, hubName)
	}
}

func (f *FakeConfigMapStore) getConfigMapDataByHubNameAndType(hubName string, configMapType string) (map[string]string, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetConfigMapByHubAndTypeErr != nil {
		return nil, false, f.GetConfigMapByHubAndTypeErr
	}

	var matches []*v1.ConfigMap
	for _, cm := range f.byHub[hubName] {
		if cm.Labels[kube.ConfigMapTypeLabel] == configMapType {
			matches = append(matches, cm)
		}
	}

	switch len(matches) {
	case 0:
		return nil, false, nil
	case 1:
		return maps.Clone(matches[0].Data), true, nil
	default:
		return nil, true, fmt.Errorf("multiple ConfigMaps found for the same hub and type: %s, %s", hubName, configMapType)
	}
}

func (f *FakeConfigMapStore) GetEnvConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return f.getConfigMapDataByHubNameAndType(hubName, kube.EnvConfigMapType)
}

func (f *FakeConfigMapStore) GetAutomationConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return f.getConfigMapDataByHubNameAndType(hubName, kube.AutomationConfigMapType)
}

func (f *FakeConfigMapStore) GetVariablesSchemaConfigMapDataByHubName(hubName string) (map[string]string, bool, error) {
	return f.getConfigMapDataByHubNameAndType(hubName, kube.VariablesSchemaMapType)
}

func (f *FakeConfigMapStore) Running() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.running
}
func (f *FakeConfigMapStore) Stopped() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stopped
}

func (f *FakeConfigMapStore) FailRunWith(err error) *FakeConfigMapStore {
	f.RunErr = err
	return f
}
func (f *FakeConfigMapStore) FailAllHubsWith(err error) *FakeConfigMapStore {
	f.GetAllHubsToDataMapErr = err
	return f
}
func (f *FakeConfigMapStore) FailAllHubsByTypeWith(err error) *FakeConfigMapStore {
	f.GetAllHubsToDataMapByTypeErr = err
	return f
}
func (f *FakeConfigMapStore) FailGetByHubWith(err error) *FakeConfigMapStore {
	f.GetConfigMapByHubNameErr = err
	return f
}

// FailGetByNameWith makes GetConfigmapByNameAndNamespace return err.
func (f *FakeConfigMapStore) FailGetByNameWith(err error) *FakeConfigMapStore {
	f.GetConfigMapByNameErr = err
	return f
}

func (f *FakeConfigMapStore) FailGetByHubAndTypeWith(err error) *FakeConfigMapStore {
	f.GetConfigMapByHubAndTypeErr = err
	return f
}

func (f *FakeConfigMapStore) SeedHub(hubName string, data map[string]string) *FakeConfigMapStore {
	return f.SeedConfigMap(hubName, hubName+"-cm", kube.EnvConfigMapType, data)
}

// SetHubConfigMaps replaces the hub's ConfigMaps with copies of cms.
func (f *FakeConfigMapStore) SetHubConfigMaps(hubName string, cms []*v1.ConfigMap) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byHub == nil {
		f.byHub = make(map[string][]*v1.ConfigMap)
	}
	copies := make([]*v1.ConfigMap, len(cms))
	for i, cm := range cms {
		copies[i] = cm.DeepCopy()
	}
	f.byHub[hubName] = copies
}
