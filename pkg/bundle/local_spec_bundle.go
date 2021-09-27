package bundle

import (
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
)

// NewLocalSpecBundle returns a new empty Local Spec bundle.
func NewLocalSpecBundle() *LocalSpecBundle {
	return &LocalSpecBundle{}
}

// LocalSpecBundle a struct used to hold the information given from bundles in the transport.
type LocalSpecBundle struct {
	Objects     []*policiesv1.Policy `json:"objects"`
	LeafHubName string               `json:"leafHubName"`
	Generation  uint64               `json:"generation"`
}

func (bundle *LocalSpecBundle) GetExplicitDependencyGeneration() uint64 {
	return NoGeneration
}

// GetLeafHubName returns the leaf hub that the bundle originated from.
func (bundle *LocalSpecBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns all the objects that the bundle hold.
func (bundle *LocalSpecBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))
	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetGeneration returns the amount of times this bundle has been updated (its generation).
func (bundle *LocalSpecBundle) GetGeneration() uint64 {
	return bundle.Generation
}
