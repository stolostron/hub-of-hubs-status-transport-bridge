package bundle

import (
	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewManagedClustersStatusBundle creates a new managed clusters  bundle with no data in it.
func NewManagedClustersStatusBundle() *ManagedClustersStatusBundle {
	return &ManagedClustersStatusBundle{
		Objects: make([]*clusterv1.ManagedCluster, 0),
	}
}

// ManagedClustersStatusBundle abstracts management of managed clusters bundle.
type ManagedClustersStatusBundle struct {
	Objects     []*clusterv1.ManagedCluster `json:"objects"`
	LeafHubName string                      `json:"leafHubName"`
	statusbundle.BundleVersion
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *ManagedClustersStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *ManagedClustersStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetDependency return the bundle dependency or nil in case there is no dependency.
func (bundle *ManagedClustersStatusBundle) GetDependency() *DependencyBundle {
	return nil
}

// GetVersion returns the bundle version.
func (bundle *ManagedClustersStatusBundle) GetVersion() *statusbundle.BundleVersion {
	return &bundle.BundleVersion
}
