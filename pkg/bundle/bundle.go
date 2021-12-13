package bundle

// CreateBundleFunction function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of objects that were sent from leaf hubs via transport layer.
type Bundle interface {
	// GetLeafHubName returns the leaf hub name that sent the bundle.
	GetLeafHubName() string
	// GetObjects returns the objects in the bundle.
	GetObjects() []interface{}
	// GetGeneration returns the bundle generation.
	GetGeneration() uint64
}

// DependantBundle is a bundle that depends on a different bundle.
// to support bundles dependencies additional function is required - GetDependencyGeneration, in order to start
// processing the dependant bundle only after it's required dependency (with required generation) was processed.
type DependantBundle interface {
	Bundle
	// GetDependencyGeneration returns the bundle dependency required generation.
	GetDependencyGeneration() uint64
}
