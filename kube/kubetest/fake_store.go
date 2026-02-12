package kubetest

import (
	"fmt"
	"sync"

	"github.com/mydecisive/mdai-data-core/kube"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ kube.ConfigMapStore = (*FakeConfigMapStore)(nil)

// FakeConfigMapStore is a threadsafe in-memory implementation of kube.ConfigMapStore.
// It avoids client-go informers and lets tests seed & assert data deterministically.
type FakeConfigMapStore struct {
	mu sync.RWMutex

	// hub -> configmaps
	byHub map[string][]*v1.ConfigMap

	// Optional error injection points
	RunErr                   error
	GetAllHubsToDataMapErr   error
	GetHubDataErr            error
	GetConfigMapByHubNameErr error

	// Lifecycle flags
	running bool
	stopped bool
}

// NewFakeConfigMapStore creates an empty fake store.
func NewFakeConfigMapStore() *FakeConfigMapStore {
	return &FakeConfigMapStore{
		byHub: make(map[string][]*v1.ConfigMap),
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
		Data: data,
	}
	f.byHub[hubName] = append(f.byHub[hubName], cm)
	return f
}

// Reset clears all state.
func (f *FakeConfigMapStore) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byHub = make(map[string][]*v1.ConfigMap)
	f.running = false
	f.stopped = false
	f.RunErr = nil
	f.GetAllHubsToDataMapErr = nil
	f.GetHubDataErr = nil
	f.GetConfigMapByHubNameErr = nil
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
		out[hub] = cms[len(cms)-1].Data
	}
	return out, nil
}

func (f *FakeConfigMapStore) GetHubData(hubName string) ([]map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.GetHubDataErr != nil {
		return nil, f.GetHubDataErr
	}

	configMaps := f.byHub[hubName]
	res := make([]map[string]string, 0, len(configMaps))
	for _, cm := range configMaps {
		res = append(res, cm.Data)
	}
	return res, nil
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
		return nil, fmt.Errorf("no ConfigMap %s found for hub: %s", "?", hubName)
	case 1:
		return configMaps[0], nil
	default:
		names := make([]string, len(configMaps))
		for i, cm := range configMaps {
			names[i] = cm.Name
		}
		return nil, fmt.Errorf("multiple ConfigMaps %v found for the same hub: %s", names, hubName)
	}
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
func (f *FakeConfigMapStore) FailGetHubDataWith(err error) *FakeConfigMapStore {
	f.GetHubDataErr = err
	return f
}
func (f *FakeConfigMapStore) FailGetByHubWith(err error) *FakeConfigMapStore {
	f.GetConfigMapByHubNameErr = err
	return f
}

func (f *FakeConfigMapStore) SeedHub(hubName string, data map[string]string) *FakeConfigMapStore {
	return f.SeedConfigMap(hubName, hubName+"-cm", kube.EnvConfigMapType, data)
}

func (f *FakeConfigMapStore) SetHubConfigMaps(hubName string, cms []*v1.ConfigMap) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byHub == nil {
		f.byHub = make(map[string][]*v1.ConfigMap)
	}
	f.byHub[hubName] = cms
}
