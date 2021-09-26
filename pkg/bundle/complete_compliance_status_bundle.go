package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

const (
	clustersPerPolicyBundleType = "ClustersPerPolicyBundle"
)

// NewCompleteComplianceStatusBundle creates a new compliance status bundle with no data in it.
func NewCompleteComplianceStatusBundle() *CompleteComplianceStatusBundle {
	return &CompleteComplianceStatusBundle{}
}

// CompleteComplianceStatusBundle abstracts management of compliance status bundle.
type CompleteComplianceStatusBundle struct {
	statusbundle.BaseCompleteComplianceStatusBundle
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

// GetVersion returns the bundle version.
func (bundle *CompleteComplianceStatusBundle) GetVersion() *statusbundle.BundleVersion {
	return &bundle.BundleVersion
}

// GetDependency return the bundle dependency or nil in case there is no dependency.
func (bundle *CompleteComplianceStatusBundle) GetDependency() *DependencyBundle {
	return &DependencyBundle{
		BundleType:    clustersPerPolicyBundleType,
		BundleVersion: statusbundle.NewBundleVersion(bundle.Incarnation, bundle.BaseBundleGeneration),
	}
}
