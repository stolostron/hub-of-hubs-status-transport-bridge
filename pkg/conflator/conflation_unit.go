package conflator

import (
	"sync"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

type ProcessingState string

const (
	none ProcessingState = "none"
	inReadyQueue ProcessingState = "inReadyQueue"
	inProcess ProcessingState = "inProcess"
)

func newConflationUnit(readyQueue *ConflationReadyQueue) *ConflationUnit {
	return &ConflationUnit{
		readyQueue: readyQueue,
		processingState: none,
		lock:       sync.Mutex{},
	}
}

// ConflationUnit abstracts the conflation of multiple bundles with dependencies between them.
type ConflationUnit struct {
	readyQueue *ConflationReadyQueue
	processingState ProcessingState
	lock       sync.Mutex
}

func (cu *ConflationUnit) insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	helpers.GetBundleType(bundle)


	if cu.processingState == none {
		cu.readyQueue.Enqueue(cu) // let the dispatcher know this CU has a ready to be processed bundle
		cu.processingState = inReadyQueue
	}
}

// GetNext returns the next ready to be processed bundle and its transport metadata.
func (cu *ConflationUnit) GetNext() (bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	cu.processingState = inProcess


	return nil, nil
}

// ReportResult is used to report the result of bundle handling job.
func (cu *ConflationUnit) ReportResult(metadata transport.BundleMetadata, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	cu.processingState = none // finished processing TODO could be also a trigger to add to ready queue
}
