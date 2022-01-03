package bundle

import (
	placementrulesv1 "github.com/open-cluster-management/multicloud-operators-placementrule/pkg/apis/apps/v1"
)

// NewLocalPlacementRulesBundle creates a new instance of LocalPlacementRulesBundle.
func NewLocalPlacementRulesBundle() Bundle {
	return &LocalPlacementRulesBundle{}
}

// LocalPlacementRulesBundle abstracts management of local placement rules bundle.
type LocalPlacementRulesBundle struct {
	baseBundle
	Objects []*placementrulesv1.PlacementRule `json:"objects"`
}

// GetObjects returns the objects in the bundle.
func (bundle *LocalPlacementRulesBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}
