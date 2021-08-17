package bundle

import (
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
)

func NewLocalSpecBundle() *LocalSpecBundle {
	return &LocalSpecBundle{}
}

type LocalSpecBundle struct {
	Objects     []*policiesv1.Policy `json:"objects"`
	LeafHubName string               `json:"leafHubName"`
	Generation  uint64               `json:"generation"`
}

func (bundle *LocalSpecBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

func (bundle *LocalSpecBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

func (bundle *LocalSpecBundle) GetGeneration() uint64 {
	return bundle.Generation
}
