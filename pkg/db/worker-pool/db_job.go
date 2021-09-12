package workerpool

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// DBJobHandlerFunc is a function for handling a db job.
type DBJobHandlerFunc func(context.Context, bundle.Bundle, db.StatusTransportBridgeDB) error

// NewDBJob creates a new instance of DBJob.
func NewDBJob(bundle bundle.Bundle, metadata transport.BundleMetadata, handlerFunction DBJobHandlerFunc,
	conflationUnit *conflator.ConflationUnit) *DBJob {
	return &DBJob{
		bundle:         bundle,
		bundleMetadata: metadata,
		handlerFunc:    handlerFunction,
		conflationUnit: conflationUnit,
	}
}

// DBJob represents the job to be run by a DBWorker from the pool.
type DBJob struct {
	bundle         bundle.Bundle
	bundleMetadata transport.BundleMetadata
	handlerFunc    DBJobHandlerFunc
	conflationUnit *conflator.ConflationUnit
}
