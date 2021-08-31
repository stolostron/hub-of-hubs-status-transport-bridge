package transport

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"

// Transport is the status bridge transport layer interface.
type Transport interface {
	// Register function registers for the bundle updates channel.
	Register(registration *BundleRegistration, bundleUpdatesChan chan bundle.Bundle)
	// CommitAsync function commits a bundle message in transport platform.
	CommitAsync(interface{})
	// Start starts the transport
	Start() error
	// Stop stops the transport
	Stop()
}
