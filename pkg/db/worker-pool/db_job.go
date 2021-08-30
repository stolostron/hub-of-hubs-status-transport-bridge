package workerpool

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

// DBJobHandlerFunc is a function for handling a db job.
type DBJobHandlerFunc func(context.Context, db.StatusTransportBridgeDB)

// DBJob represents the job to be run by a dbWorker from the pool.
type DBJob struct {
	HandlerFunc DBJobHandlerFunc
}
