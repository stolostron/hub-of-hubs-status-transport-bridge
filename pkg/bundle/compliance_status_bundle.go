package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewComplianceStatusBundle creates a new compliance status bundle with no data in it.
func NewComplianceStatusBundle() *ComplianceStatusBundle {
	return &ComplianceStatusBundle{}
}

// ComplianceStatusBundle abstracts management of compliance status bundle.
type ComplianceStatusBundle struct {
	statusbundle.BaseComplianceStatusBundle
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

// GetExplicitDependencyGeneration returns the bundle explicit dependency generation or NoGeneration in case there is no
// dependency. in case the dependency is implicit, returns NoGeneration as well.
func (bundle *ComplianceStatusBundle) GetExplicitDependencyGeneration() uint64 {
	return bundle.BaseBundleGeneration
}

// GetGeneration returns the bundle generation.
func (bundle *ComplianceStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}
