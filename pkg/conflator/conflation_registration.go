package conflator

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

// BundleHandlerFunc is a function for handling a bundle.
type BundleHandlerFunc func(context.Context, bundle.Bundle, db.StatusTransportBridgeDB) error

// ConflationRegistration is used to register a new conflated bundle type along with its priority and handler function.
type ConflationRegistration struct {
	Priority        conflationPriority
	BundleType      string
	HandlerFunction BundleHandlerFunc
}
