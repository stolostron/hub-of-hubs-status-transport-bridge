package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewCompleteComplianceStatusBundle creates a new instance of CompleteComplianceStatusBundle.
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

// GetGeneration returns the bundle generation.
func (bundle *CompleteComplianceStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}
