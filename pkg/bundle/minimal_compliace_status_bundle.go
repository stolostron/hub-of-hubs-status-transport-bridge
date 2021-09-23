package bundle

import statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"

// NewMinimalComplianceStatusBundle creates a new minimal compliance status bundle with no data in it.
func NewMinimalComplianceStatusBundle() *MinimalComplianceStatusBundle {
	return &MinimalComplianceStatusBundle{}
}

// MinimalComplianceStatusBundle abstracts management of minimal compliance status bundle.
type MinimalComplianceStatusBundle struct {
	statusbundle.BaseMinimalComplianceStatusBundle
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

// GetDependency return the bundle dependency or nil in case there is no dependency.
func (bundle *MinimalComplianceStatusBundle) GetDependency() *DependencyBundle {
	return nil
}

// GetGeneration returns the bundle generation.
func (bundle *MinimalComplianceStatusBundle) GetGeneration() (uint64, uint64) {
	return bundle.Incarnation, bundle.Generation
}
