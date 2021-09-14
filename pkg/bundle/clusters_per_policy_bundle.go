package bundle

import statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"

// NewClustersPerPolicyBundle creates a new clusters per policy bundle with no data in it.
func NewClustersPerPolicyBundle() *ClustersPerPolicyBundle {
	return &ClustersPerPolicyBundle{}
}

// ClustersPerPolicyBundle abstracts management of clusters per policy bundle.
type ClustersPerPolicyBundle struct {
	statusbundle.BaseClustersPerPolicyBundle
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

// GetDependency return the bundle dependency or nil in case there is no dependency.
func (bundle *ClustersPerPolicyBundle) GetDependency() *DependencyBundle {
	return nil
}

// GetGeneration returns the bundle generation.
func (bundle *ClustersPerPolicyBundle) GetGeneration() uint64 {
	return bundle.Generation
}
