package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewCompleteComplianceStatusBundle creates a new instance of CompleteComplianceStatusBundle.
func NewCompleteComplianceStatusBundle() Bundle {
	return &CompleteComplianceStatusBundle{}
}

// CompleteComplianceStatusBundle abstracts management of complete compliance status bundle.
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

// GetDependencyVersion returns the bundle dependency required version.
func (bundle *CompleteComplianceStatusBundle) GetDependencyVersion() *statusbundle.BundleVersion {
	return bundle.BaseBundleVersion
}

// GetVersion returns the bundle version.
func (bundle *CompleteComplianceStatusBundle) GetVersion() *statusbundle.BundleVersion {
	return bundle.BundleVersion
}
