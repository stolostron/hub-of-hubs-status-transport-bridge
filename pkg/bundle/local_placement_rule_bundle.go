package bundle

import (
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/apps/v1"
)

// NewLocalPlacementRuleBundle returns a new empty localPlacementRule bundle.
func NewLocalPlacementRuleBundle() *LocalPlacementRuleBundle {
	return &LocalPlacementRuleBundle{}
}

// LocalPlacementRuleBundle a bundle to hold Local Placement Rules information.
type LocalPlacementRuleBundle struct {
	Objects     []*policiesv1.PlacementRule `json:"objects"`
	LeafHubName string                      `json:"leafHubName"`
	Generation  uint64                      `json:"generation"`
}

// GetLeafHubName return the leaf hub that sent the bundle.
func (bundle *LocalPlacementRuleBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects return all the objects that the bundle holds.
func (bundle *LocalPlacementRuleBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetGeneration returns how many updates the bundle passed (its generation).
func (bundle *LocalPlacementRuleBundle) GetGeneration() uint64 {
	return bundle.Generation
}
