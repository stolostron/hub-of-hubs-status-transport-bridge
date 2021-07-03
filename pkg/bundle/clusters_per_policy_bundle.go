package bundle

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

func NewClustersPerPolicyBundle() *ClustersPerPolicyBundle {
	return &ClustersPerPolicyBundle{}
}

type ClustersPerPolicyBundle struct {
	statusbundle.BaseClustersPerPolicyBundle
}

func (bundle *ClustersPerPolicyBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

func (bundle *ClustersPerPolicyBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}
	return result
}

func (bundle *ClustersPerPolicyBundle) GetGeneration() uint64 {
	return bundle.Generation
}
