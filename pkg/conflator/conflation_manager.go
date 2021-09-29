package conflator

import (
	"sync"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewConflationManager creates a new instance of ConflationManager.
func NewConflationManager(conflationUnitsReadyQueue *ConflationReadyQueue,
	statistics *statistics.Statistics) *ConflationManager {
	return &ConflationManager{
		conflationUnits: make(map[string]*ConflationUnit), // map from leaf hub to conflation unit
		registrations:   make([]*ConflationRegistration, 0),
		readyQueue:      conflationUnitsReadyQueue,
		lock:            sync.Mutex{}, // lock to be used to find/create conflation units
		statistics:      statistics,
	}
}

// ConflationManager implements conflation units management.
type ConflationManager struct {
	conflationUnits map[string]*ConflationUnit // map from leaf hub to conflation unit
	registrations   []*ConflationRegistration
	readyQueue      *ConflationReadyQueue
	lock            sync.Mutex
	statistics      *statistics.Statistics
}

// Register registers bundle type with priority and handler function within the conflation manager.
func (cm *ConflationManager) Register(registration *ConflationRegistration) {
	cm.registrations = append(cm.registrations, registration)
}

// Insert function inserts the bundle to the appropriate conflation unit.
func (cm *ConflationManager) Insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cm.getConflationUnit(bundle.GetLeafHubName()).insert(bundle, metadata)
}

// if conflation unit doesn't exist for leaf hub, creates it.
func (cm *ConflationManager) getConflationUnit(leafHubName string) *ConflationUnit {
	cm.lock.Lock() // use lock to find/create conflation units
	defer cm.lock.Unlock()

	if conflationUnit, found := cm.conflationUnits[leafHubName]; found {
		return conflationUnit
	}
	// otherwise, need to create conflation unit
	conflationUnit := newConflationUnit(cm.readyQueue, cm.registrations, cm.statistics)
	cm.conflationUnits[leafHubName] = conflationUnit

	return conflationUnit
}
