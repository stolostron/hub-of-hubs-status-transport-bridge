package bundle

import "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetDependency() *DependencyBundle
	GetVersion() *status.BundleVersion
}

// DependencyBundle represents the dependency between different bundles.
type DependencyBundle struct {
	RequiresExactVersion bool
	BundleType           string
	*status.BundleVersion
}
