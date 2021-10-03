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

// GetGeneration returns the generation of the bundle.
func (bundle *LocalSpecBundle) GetGeneration() uint64 {
	return bundle.Generation
}
