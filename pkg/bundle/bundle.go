package bundle

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetDependency() *DependencyBundle
	// GetGeneration returns a tuple of (INCARNATION,GENERATION) where the first increments on every sender restart,
	// the latter begins from 0 on every restart and increments on every bundle type update.
	//	This is necessary in order to correctly infer bundle age when their senders are different instances.
	GetGeneration() (uint64, uint64)
}

// DependencyBundle represents the dependency between different bundles.
type DependencyBundle struct {
	BundleType  string
	Incarnation uint64
	Generation  uint64
}
