package conflator

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

// BundleHandlerFunc is a function for handling a bundle.
type BundleHandlerFunc func(context.Context, bundle.Bundle, db.StatusTransportBridgeDB) error

// NewConflationRegistration creates a new instance of ConflationRegistration.
func NewConflationRegistration(priority conflationPriority, bundleType string,
	handlerFunction BundleHandlerFunc) *ConflationRegistration {
	return &ConflationRegistration{
		priority:        priority,
		bundleType:      bundleType,
		handlerFunction: handlerFunction,
		dependency:      nil,
	}
}

// ConflationRegistration is used to register a new conflated bundle type along with its priority and handler function.
type ConflationRegistration struct {
	priority        conflationPriority
	bundleType      string
	handlerFunction BundleHandlerFunc
	dependency      *dependency.Dependency
}

// WithDependency declares a dependency required by the given bundle type.
func (registration *ConflationRegistration) WithDependency(dependency *dependency.Dependency) *ConflationRegistration {
	registration.dependency = dependency
	return registration
}
