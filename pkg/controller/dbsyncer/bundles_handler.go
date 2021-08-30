package dbsyncer

import (
	"context"
	"time"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
)

type (
	// bundleHandlerFunc is a function to handle incoming bundle.
	bundleHandlerFunc func(ctx context.Context, dbConn db.StatusTransportBridgeDB, bundle bundle.Bundle) error
	successFunc       func(bundle bundle.Bundle)
	errorFunc         func(bundle bundle.Bundle, err error)
)

const (
	retrySleepIntervalSeconds = 3
)

func handleBundle(bundle bundle.Bundle, bundlesAttemptedGenerationLog *bundlesGenerationLog,
	handlerFunc bundleHandlerFunc, dbWorkerPool *workerpool.DBWorkerPool, successFunc successFunc,
	errorFunc errorFunc) {
	if bundlesAttemptedGenerationLog.getLastGeneration(bundle) > bundle.GetGeneration() {
		return // do not try to process if a newer generation processing attempted (might not finished yet)
	}

	bundlesAttemptedGenerationLog.updateLastGeneration(bundle) // update our attempt in generation log

	dbWorkerPool.QueueDBJob(&workerpool.DBJob{
		HandlerFunc: func(ctx context.Context, dbConn db.StatusTransportBridgeDB) {
			if err := handlerFunc(ctx, dbConn, bundle); err != nil {
				errorFunc(bundle, err)
			} else {
				successFunc(bundle)
			}
		},
	})
}

// handleRetry function to handle retries.
// retry is done is a different go routine to free the db worker.
func handleRetry(bundle bundle.Bundle, bundleUpdatesChan chan bundle.Bundle) {
	go func() {
		time.Sleep(time.Second * retrySleepIntervalSeconds) // TODO reschedule, should use exponential back off
		bundleUpdatesChan <- bundle
	}()
}
