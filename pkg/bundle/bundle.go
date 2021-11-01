package bundle

import "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetVersion() *status.BundleVersion
}

// DependantBundle is a bundle that depends on a different bundle.
// to support bundles dependencies additional functions are required:
//
// GetDependencyVersion- in order to start processing the dependant bundle only after it's required dependency
// (with required version) was processed.
//
// RequiresExactDependencyVersion- determines whether the dependant bundle requires the exact version of the dependency
// to be the last-processed (=), or has a version that is at least the required version (>=).
type DependantBundle interface {
	Bundle
	GetDependencyVersion() *status.BundleVersion
	RequiresExactDependencyVersion() bool
}
