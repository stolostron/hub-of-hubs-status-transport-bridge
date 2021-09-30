package conflator

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

// BundleHandlerFunc is a function for handling a bundle.
type BundleHandlerFunc func(context.Context, bundle.Bundle, db.StatusTransportBridgeDB) error

// ImplicitToExplicitDependencyFunc is a function that runs when handler returns an error, to decide if the error
// was a result of an implicit dependency that should turn into an explicit dependency to avoid multiple errors from
// the same reason.
type ImplicitToExplicitDependencyFunc func(error) bool

// NewConflationRegistration creates a new instance of ConflationRegistration.
func NewConflationRegistration(priority conflationPriority, bundleType string, handlerFunction BundleHandlerFunc,
	dependency *dependency.Dependency,
	implicitToExplicitDependencyFunc ImplicitToExplicitDependencyFunc) *ConflationRegistration {
	return &ConflationRegistration{
		Priority:                         priority,
		BundleType:                       bundleType,
		HandlerFunction:                  handlerFunction,
		dependency:                       dependency,
		implicitToExplicitDependencyFunc: implicitToExplicitDependencyFunc,
	}
}

// ConflationRegistration is used to register a new conflated bundle type along with its priority and handler function.
type ConflationRegistration struct {
	Priority                         conflationPriority
	BundleType                       string
	HandlerFunction                  BundleHandlerFunc
	dependency                       *dependency.Dependency
	implicitToExplicitDependencyFunc ImplicitToExplicitDependencyFunc
}
