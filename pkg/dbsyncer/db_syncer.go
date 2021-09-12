package dbsyncer

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/dispatcher"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// DBSyncer interface for registering business logic needed for handling bundles.
type DBSyncer interface {
	// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
	RegisterCreateBundleFunctions(transportInstance transport.Transport)
	// RegisterBundleHandlerFunctions registers bundle handler functions within the dispatcher.
	RegisterBundleHandlerFunctions(dispatcher *dispatcher.Dispatcher)
}
