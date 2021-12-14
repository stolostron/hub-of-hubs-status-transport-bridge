package workerpool

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
)

// NewDBJob creates a new instance of DBJob.
func NewDBJob(bundle bundle.Bundle, metadata *conflator.BundleMetadata, handlerFunction conflator.BundleHandlerFunc,
	conflationUnitResultReporter conflator.ResultReporter) *DBJob {
	return &DBJob{
		bundle:                       bundle,
		bundleMetadata:               metadata,
		handlerFunc:                  handlerFunction,
		conflationUnitResultReporter: conflationUnitResultReporter,
	}
}

// DBJob represents the job to be run by a DBWorker from the pool.
type DBJob struct {
	bundle                       bundle.Bundle
	bundleMetadata               *conflator.BundleMetadata
	handlerFunc                  conflator.BundleHandlerFunc
	conflationUnitResultReporter conflator.ResultReporter
}
