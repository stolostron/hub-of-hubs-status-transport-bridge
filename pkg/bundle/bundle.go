package bundle

import "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"

// CreateBundleFunction function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of objects that were sent from leaf hubs via transport layer.
type Bundle interface {
	// GetLeafHubName returns the leaf hub name that sent the bundle.
	GetLeafHubName() string
	// GetObjects returns the objects in the bundle.
	GetObjects() []interface{}
	// GetVersion returns the bundle version.
	GetVersion() *status.BundleVersion
}

// DependantBundle is a bundle that depends on a different bundle.
// to support bundles dependencies additional function is required - GetDependencyVersion, in order to start
// processing the dependant bundle only after it's required dependency (with required version) was processed.
type DependantBundle interface {
	Bundle
	// GetDependencyVersion returns the bundle dependency required version.
	GetDependencyVersion() *status.BundleVersion
}

// DeltaStateBundle abstracts the functionality required from a Bundle to be used as Delta-State bundle.
type DeltaStateBundle interface {
	DependantBundle
	// InheritEvents inherits the events in an older delta-bundle into the receiver (in-case of conflict, the receiver
	// is the source of truth).
	InheritEvents(olderBundle Bundle) error
}
