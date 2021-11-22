package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewCompleteComplianceStatusBundle creates a new complete compliance status bundle with no data in it.
func NewCompleteComplianceStatusBundle() *CompleteComplianceStatusBundle {
	return &CompleteComplianceStatusBundle{
		dependencyVersion: nil,
	}
}

// CompleteComplianceStatusBundle abstracts management of complete compliance status bundle.
type CompleteComplianceStatusBundle struct {
	statusbundle.BaseCompleteComplianceStatusBundle
	dependencyVersion *statusbundle.BundleVersion
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *CompleteComplianceStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *CompleteComplianceStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetDependencyVersion returns the bundle dependency required version.
func (bundle *CompleteComplianceStatusBundle) GetDependencyVersion() *statusbundle.BundleVersion {
	// incarnation version of the dependency bundle has to be equal to that of the dependent bundle (same sender),
	// therefore it is omitted from the bundle itself (only generation is maintained).
	if bundle.dependencyVersion == nil {
		bundle.dependencyVersion = statusbundle.NewBundleVersion(bundle.Incarnation, bundle.BaseBundleGeneration)
	}

	return bundle.dependencyVersion
}

// GetVersion returns the bundle version.
func (bundle *CompleteComplianceStatusBundle) GetVersion() *statusbundle.BundleVersion {
	return &bundle.BundleVersion
}
