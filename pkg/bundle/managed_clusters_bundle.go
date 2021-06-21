package bundle

func NewManagedClustersStatusBundle() *ManagedClustersStatusBundle {
	return &ManagedClustersStatusBundle{
		Objects: make([]Object, 0),
	}
}

type ManagedClustersStatusBundle struct {
	Objects   []Object `json:"objects"`
	LeafHubId string   `json:"leafHubId"`
}

func (bundle *ManagedClustersStatusBundle) GetLeafHubId() string {
	return bundle.LeafHubId
}

func (bundle *ManagedClustersStatusBundle) GetObjects() []Object {
	return bundle.Objects
}
