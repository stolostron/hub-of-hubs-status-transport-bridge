package bundle

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle/object"

func NewManagedClustersStatusBundle() *ManagedClustersStatusBundle {
	return &ManagedClustersStatusBundle{
		Objects: make([]*object.TimestampedManagedCluster, 0),
	}
}

type ManagedClustersStatusBundle struct {
	Objects   []*object.TimestampedManagedCluster `json:"objects"`
	LeafHubId string                              `json:"leafHubId"`
}

func (bundle *ManagedClustersStatusBundle) GetLeafHubId() string {
	return bundle.LeafHubId
}

func (bundle *ManagedClustersStatusBundle) GetObjects() []object.Object {
	result := make([]object.Object, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}
	return result
}
