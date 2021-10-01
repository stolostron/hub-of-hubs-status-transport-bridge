package bundle

import (
	"time"

	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewMinimalComplianceStatusBundle creates a new minimal compliance status bundle with no data in it.
func NewMinimalComplianceStatusBundle() *MinimalComplianceStatusBundle {
	return &MinimalComplianceStatusBundle{}
}

// MinimalComplianceStatusBundle abstracts management of minimal compliance status bundle.
type MinimalComplianceStatusBundle struct {
	statusbundle.BaseMinimalComplianceStatusBundle
	conflationUnitInsertTime time.Time
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *MinimalComplianceStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *MinimalComplianceStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetGeneration returns the bundle generation.
func (bundle *MinimalComplianceStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}

// SetConflationUnitInsertTime sets a time the bundle was inserted into conflation unit.
func (bundle *MinimalComplianceStatusBundle) SetConflationUnitInsertTime(time time.Time) {
	bundle.conflationUnitInsertTime = time
}

// GetConflationUnitInsertTime gets a time the bundle was inserted into conflation unit.
func (bundle *MinimalComplianceStatusBundle) GetConflationUnitInsertTime() time.Time {
	return bundle.conflationUnitInsertTime
}
