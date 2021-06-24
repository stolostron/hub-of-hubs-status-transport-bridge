package bundle

import (
	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
)

func NewManagedClustersStatusBundle() *ManagedClustersStatusBundle {
	return &ManagedClustersStatusBundle{
		Objects: make([]*clusterv1.ManagedCluster, 0),
	}
}

type ManagedClustersStatusBundle struct {
	Objects     []*clusterv1.ManagedCluster `json:"objects"`
	LeafHubName string                      `json:"leafHubName"`
}

func (bundle *ManagedClustersStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

func (bundle *ManagedClustersStatusBundle) GetObjects() []Object {
	result := make([]Object, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}
	return result
}
