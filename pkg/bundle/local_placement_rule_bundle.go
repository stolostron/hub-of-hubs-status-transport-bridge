package bundle

import (
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
)

func NewLocalPlacementRuleBundle() *LocalPlacementRuleBundle {
	return &LocalPlacementRuleBundle{}
}

type LocalPlacementRuleBundle struct {
	Objects     []*policiesv1.PlacementRule `json:"objects"`
	LeafHubName string                      `json:"leafHubName"`
	Generation  uint64                      `json:"generation"`
}

func (bundle *LocalPlacementRuleBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

func (bundle *LocalPlacementRuleBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

func (bundle *LocalPlacementRuleBundle) GetGeneration() uint64 {
	return bundle.Generation
}
