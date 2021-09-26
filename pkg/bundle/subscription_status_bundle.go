package bundle

import (
	subv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
)

// NewSubscriptionBundle returns a new empty localPlacementRule bundle.
func NewSubscriptionBundle() *SubscriptionStatusBundle {
	return &SubscriptionStatusBundle{}
}

// SubscriptionStatusBundle a bundle to hold Local Placement Rules information.
type SubscriptionStatusBundle struct {
	Objects     []*subv1.Subscription `json:"objects"`
	LeafHubName string                `json:"leafHubName"`
	Generation  uint64                `json:"generation"`
}

// GetLeafHubName return the leaf hub that sent the bundle.
func (bundle *SubscriptionStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects return all the objects that the bundle holds.
func (bundle *SubscriptionStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetGeneration returns how many updates the bundle passed (its generation).
func (bundle *SubscriptionStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}

// TODO: check if there are dependencies

// GetDependency returns the dependencies of the bundle.
func (bundle *SubscriptionStatusBundle) GetDependency() *DependencyBundle {
	return nil
}
