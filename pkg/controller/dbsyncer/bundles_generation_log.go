package dbsyncer

import (
	"sync"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/helpers"
)

func newBundlesGenerationLog() *bundlesGenerationLog {
	return &bundlesGenerationLog{
		generationLog: make(map[string]map[string]uint64), // map from bundle type -> leaf hub name -> generation
		lock:          sync.Mutex{},
	}
}

type bundlesGenerationLog struct {
	generationLog map[string]map[string]uint64 // map from bundle type -> leaf hub name -> generation
	lock          sync.Mutex
}

func (b *bundlesGenerationLog) getLastGeneration(bundle bundle.Bundle) uint64 {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.generationLog[helpers.GetBundleType(bundle)][bundle.GetLeafHubName()]
}

// updates generation only it is bigger.
func (b *bundlesGenerationLog) updateLastGeneration(bundle bundle.Bundle) {
	b.lock.Lock()
	defer b.lock.Unlock()

	bundleType := helpers.GetBundleType(bundle)
	leafHubName := bundle.GetLeafHubName()

	if _, found := b.generationLog[bundleType]; !found {
		b.generationLog[bundleType] = make(map[string]uint64)
	}

	if _, found := b.generationLog[bundleType][leafHubName]; !found {
		b.generationLog[bundleType][leafHubName] = uint64(0)
	}

	if bundle.GetGeneration() > b.generationLog[bundleType][leafHubName] {
		b.generationLog[bundleType][leafHubName] = bundle.GetGeneration()
	}
}
