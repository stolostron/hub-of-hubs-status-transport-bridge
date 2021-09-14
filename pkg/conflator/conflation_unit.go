package conflator

import (
	"errors"
	"sync"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const (
	invalidPriority = -1
)

var errNoReadyBundle = errors.New("no bundle is ready to be processed")

// ResultReporter is an interface used to report the result of the handler function after it's invocation.
// the idea is to have a clear separation of concerns and make sure dispatcher can only request for bundles and
// DB workers can only report results and not request for additional bundles.
// this makes sure DB workers get their input only via the dispatcher which is the entity responsible for reading
// bundles and invoking the handler functions using DB jobs.
// (using this interfaces verifies no developer violates the design that was intended).
type ResultReporter interface {
	ReportResult(metadata *BundleMetadata, err error)
}

func newConflationUnit(readyQueue *ConflationReadyQueue, registrations []*ConflationRegistration) *ConflationUnit {
	priorityQueue := make([]*conflationElement, len(registrations))
	bundleTypeToQueueIndex := make(map[string]conflationPriority)

	for _, registration := range registrations {
		priorityQueue[registration.Priority] = &conflationElement{
			bundleType:                    registration.BundleType,
			bundle:                        nil,
			bundleMetadata:                nil,
			handlerFunction:               registration.HandlerFunction,
			lastProcessedBundleGeneration: 0,
		}
		bundleTypeToQueueIndex[registration.BundleType] = registration.Priority
	}

	return &ConflationUnit{
		priorityQueue:        priorityQueue,
		bundleTypeToPriority: bundleTypeToQueueIndex,
		readyQueue:           readyQueue,
		processingState:      none,
		lock:                 sync.Mutex{},
	}
}

// ConflationUnit abstracts the conflation of prioritized multiple bundles with dependencies between them.
type ConflationUnit struct {
	priorityQueue        []*conflationElement
	bundleTypeToPriority map[string]conflationPriority
	readyQueue           *ConflationReadyQueue
	processingState      conflationUnitProcessingState
	lock                 sync.Mutex
}

func (cu *ConflationUnit) insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundleType := helpers.GetBundleType(bundle)
	priority := cu.bundleTypeToPriority[bundleType]

	if cu.priorityQueue[priority].bundle != nil && // we have bundle in CU for this type
		bundle.GetGeneration() <= cu.priorityQueue[priority].bundle.GetGeneration() {
		return // update bundles only if the generation we got is newer, otherwise do nothing.
	}

	cu.cleanStaleBundles(bundleType, cu.priorityQueue[priority].bundle.GetGeneration())

	// if we got here, we got newer generation
	// NOTICE - we replace pointers and not override the values inside the pointers for not changing bundles/metadata
	// that were already given to DB workers for processing.
	cu.priorityQueue[priority].bundle = bundle // update the bundle in the priority queue.
	if cu.priorityQueue[priority].bundleMetadata != nil && cu.processingState != inProcess {
		cu.priorityQueue[priority].bundleMetadata.update(bundle.GetGeneration(), metadata)
	} else {
		cu.priorityQueue[priority].bundleMetadata = &BundleMetadata{
			bundleType:              bundleType,
			generation:              bundle.GetGeneration(),
			transportBundleMetadata: metadata,
		}
	}

	cu.addCUToReadyQueueIfNeeded()
}

// GetNext returns the next ready to be processed bundle and its transport metadata.
func (cu *ConflationUnit) GetNext() (bundle bundle.Bundle, metadata *BundleMetadata, handlerFunc BundleHandlerFunc,
	err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	nextBundleToProcessPriority := cu.getNextReadyBundlePriority()
	if nextBundleToProcessPriority == invalidPriority { // CU adds itself to RQ only when it has ready to process bundle
		return nil, nil, nil, errNoReadyBundle // therefore this shouldn't happen
	}

	cu.processingState = inProcess
	bundleInfo := cu.priorityQueue[nextBundleToProcessPriority]

	return bundleInfo.bundle, bundleInfo.bundleMetadata, bundleInfo.handlerFunction, nil
}

// ReportResult is used to report the result of bundle handling job.
func (cu *ConflationUnit) ReportResult(metadata *BundleMetadata, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	cu.processingState = none // finished processing this bundle, can check if more bundles are ready to be processed.

	if err != nil {
		cu.addCUToReadyQueueIfNeeded()
		return
	}
	// otherwise err is nil, means bundle processing finished successfully
	priority := cu.bundleTypeToPriority[metadata.bundleType]
	if metadata.generation > cu.priorityQueue[priority].lastProcessedBundleGeneration {
		cu.priorityQueue[priority].lastProcessedBundleGeneration = metadata.generation
	}
	// if bundle wasn't updated since GetNext was called - delete bundle + metadata since it was already processed
	if metadata.generation == cu.priorityQueue[priority].bundle.GetGeneration() {
		cu.priorityQueue[priority].bundle = nil
		cu.priorityQueue[priority].bundleMetadata = nil
	}

	// TODO metadata should be sent to transport committer when the component is implemented
	// transportCommitter.MarkAsConsumed(metadata)

	cu.addCUToReadyQueueIfNeeded()
}

func (cu *ConflationUnit) addCUToReadyQueueIfNeeded() {
	if cu.processingState == inReadyQueue || cu.processingState == inProcess {
		return // allow CU to appear only once in RQ/processing
	}
	// if we reached here, processing state is none.
	nextReadyBundlePriority := cu.getNextReadyBundlePriority()
	if nextReadyBundlePriority != invalidPriority { // there is a ready to be processed bundle
		cu.readyQueue.Enqueue(cu) // let the dispatcher know this CU has a ready to be processed bundle
		cu.processingState = inReadyQueue
	}
}

func (cu *ConflationUnit) getNextReadyBundlePriority() int {
	for priority, conflationElement := range cu.priorityQueue { // going over priority queue according to priorities.
		if conflationElement.bundle != nil && cu.checkDependencies(conflationElement.bundle) {
			return priority // bundle in this priority is ready to be processed
		}
	}

	return invalidPriority
}

// dependencies are organized in a chain.
func (cu *ConflationUnit) checkDependencies(bundleToCheck bundle.Bundle) bool {
	dependency := bundleToCheck.GetDependency()
	if dependency == nil {
		return true
	}

	dependencyIndex := cu.bundleTypeToPriority[dependency.BundleType]
	if dependency.Generation > cu.priorityQueue[dependencyIndex].lastProcessedBundleGeneration {
		return false // the needed dependency generation wasn't processed yet
	}

	return cu.checkDependencies(cu.priorityQueue[dependencyIndex].bundle)
}

// this functions cleans stale bundles that depends on the given (bundleType, generation) along the dependency chain.
func (cu *ConflationUnit) cleanStaleBundles(bundleType string, generation uint64) {
	// TODO
}
