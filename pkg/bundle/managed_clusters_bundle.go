package bundle

func NewManagedClustersStatusBundle() *ManagedClustersStatusBundle {
	return &ManagedClustersStatusBundle{
		Objects: make([]*TimestampedManagedCluster, 0),
	}
}

type ManagedClustersStatusBundle struct {
	Objects   []*TimestampedManagedCluster `json:"objects"`
	LeafHubId string                       `json:"leafHubId"`
}

func (bundle *ManagedClustersStatusBundle) GetLeafHubId() string {
	return bundle.LeafHubId
}

func (bundle *ManagedClustersStatusBundle) GetObjects() []Object {
	result := make([]Object, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}
	return result
}
