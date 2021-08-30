package dbsyncer

import (
	"sync"
)

func newLeafHubsLocks() *leafHubsLocks {
	return &leafHubsLocks{
		leafHubsLocks: make(map[string]*sync.Mutex), // map from leaf hub name -> lock
		lock:          sync.Mutex{},
	}
}

type leafHubsLocks struct {
	leafHubsLocks map[string]*sync.Mutex // map from leaf hub name -> lock
	lock          sync.Mutex
}

func (locks *leafHubsLocks) lockLeafHub(leafHubName string) {
	locks.lock.Lock()
	defer locks.lock.Unlock()

	if _, found := locks.leafHubsLocks[leafHubName]; !found {
		locks.leafHubsLocks[leafHubName] = &sync.Mutex{}
	}

	locks.leafHubsLocks[leafHubName].Lock()
}

func (locks *leafHubsLocks) unlockLeafHub(leafHubName string) {
	locks.lock.Lock()
	defer locks.lock.Unlock()

	if _, found := locks.leafHubsLocks[leafHubName]; !found {
		return
	}

	locks.leafHubsLocks[leafHubName].Unlock()
}
