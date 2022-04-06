package bundle

import (
	statusbundle "github.com/stolostron/hub-of-hubs-data-types/bundle/status"
)

// NewPoliciesPlacementBundle creates a new instance of PoliciesPlacementBundle.
func NewPoliciesPlacementBundle() Bundle {
	return &PoliciesPlacementBundle{}
}

// PoliciesPlacementBundle abstracts management of policies placement bundle.
type PoliciesPlacementBundle struct {
	baseBundle
	Objects []*statusbundle.PolicyPlacementStatus `json:"objects"`
}

// GetObjects returns the objects in the bundle.
func (bundle *PoliciesPlacementBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}
