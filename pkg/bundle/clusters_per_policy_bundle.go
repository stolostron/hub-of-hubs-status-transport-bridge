package bundle

import (
	"time"

	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewClustersPerPolicyBundle creates a new clusters per policy bundle with no data in it.
func NewClustersPerPolicyBundle() *ClustersPerPolicyBundle {
	return &ClustersPerPolicyBundle{}
}

// ClustersPerPolicyBundle abstracts management of clusters per policy bundle.
type ClustersPerPolicyBundle struct {
	statusbundle.BaseClustersPerPolicyBundle
	conflationUnitInsertTime time.Time
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *ClustersPerPolicyBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *ClustersPerPolicyBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetGeneration returns the bundle generation.
func (bundle *ClustersPerPolicyBundle) GetGeneration() uint64 {
	return bundle.Generation
}

// SetConflationUnitInsertTime sets a time the bundle was inserted into conflation unit.
func (bundle *ClustersPerPolicyBundle) SetConflationUnitInsertTime(time time.Time) {
	bundle.conflationUnitInsertTime = time
}

// GetConflationUnitInsertTime gets a time the bundle was inserted into conflation unit.
func (bundle *ClustersPerPolicyBundle) GetConflationUnitInsertTime() time.Time {
	return bundle.conflationUnitInsertTime
}
