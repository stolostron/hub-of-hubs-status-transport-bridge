package bundle

const (
	// NoGeneration is used to indicate no generation for bundle (e.g. dependency generation for bundle with no
	// dependency).
	NoGeneration = 0
)

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetExplicitDependencyGeneration() uint64
	GetGeneration() uint64
}
