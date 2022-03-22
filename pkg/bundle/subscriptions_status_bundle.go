package bundle

import (
	subscriptionsv1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/apps/v1"
)

// NewSubscriptionsStatusBundle creates a new instance of SubscriptionsStatusBundle.
func NewSubscriptionsStatusBundle() Bundle {
	return &SubscriptionsStatusBundle{}
}

// SubscriptionsStatusBundle abstracts management of subscriptions status bundle.
type SubscriptionsStatusBundle struct {
	baseBundle
	Objects []*subscriptionsv1.Subscription `json:"objects"`
}

// GetObjects return all the objects that the bundle holds.
func (bundle *SubscriptionsStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}
