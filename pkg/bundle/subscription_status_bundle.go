package bundle

import (
	subv1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/apps/v1"
)

// NewSubscriptionBundle returns a new empty localPlacementRule bundle.
func NewSubscriptionBundle() *SubscriptionStatusBundle {
	return &SubscriptionStatusBundle{}
}

// SubscriptionStatusBundle a bundle to hold Local Placement Rules information.
type SubscriptionStatusBundle struct {
	baseBundle
	Objects []*subv1.Subscription `json:"objects"`
}

// GetObjects return all the objects that the bundle holds.
func (bundle *SubscriptionStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}
