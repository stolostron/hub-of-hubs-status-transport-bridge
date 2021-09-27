package bundle

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetDependency() *DependencyBundle
	GetGeneration() uint64
}

// DependencyBundle represents the dependency between different bundles.
type DependencyBundle struct {
	BundleType string
	Generation uint64
}
