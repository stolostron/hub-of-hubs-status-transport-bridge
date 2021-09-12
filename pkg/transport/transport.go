package transport

// Transport is the status bridge transport layer interface.
type Transport interface {
	// Register function registers a msgID for sync service to know how to create the bundle, and use predicate.
	Register(registration *BundleRegistration)
}
