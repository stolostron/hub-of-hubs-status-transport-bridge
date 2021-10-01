package bundle

import (
	"time"

	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewComplianceStatusBundle creates a new compliance status bundle with no data in it.
func NewComplianceStatusBundle() *ComplianceStatusBundle {
	return &ComplianceStatusBundle{}
}

// ComplianceStatusBundle abstracts management of compliance status bundle.
type ComplianceStatusBundle struct {
	statusbundle.BaseComplianceStatusBundle
	conflationUnitInsertTime time.Time
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *ComplianceStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *ComplianceStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetDependencyGeneration returns the bundle dependency required generation.
func (bundle *ComplianceStatusBundle) GetDependencyGeneration() uint64 {
	return bundle.BaseBundleGeneration
}

// GetGeneration returns the bundle generation.
func (bundle *ComplianceStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}

// SetConflationUnitInsertTime sets a time the bundle was inserted into conflation unit.
func (bundle *ComplianceStatusBundle) SetConflationUnitInsertTime(time time.Time) {
	bundle.conflationUnitInsertTime = time
}

// GetConflationUnitInsertTime gets a time the bundle was inserted into conflation unit.
func (bundle *ComplianceStatusBundle) GetConflationUnitInsertTime() time.Time {
	return bundle.conflationUnitInsertTime
}
