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
	bundleTypeToPriority := make(map[string]conflationPriority)

	for _, registration := range registrations {
		priorityQueue[registration.Priority] = &conflationElement{
			bundleType:                     registration.BundleType,
			bundle:                         nil,
			bundleMetadata:                 nil,
			handlerFunction:                registration.HandlerFunction,
			isInProcess:                    false,
			lastProcessedBundleIncarnation: 0,
			lastProcessedBundleGeneration:  0, // no generation was processed yet
		}
		bundleTypeToPriority[registration.BundleType] = registration.Priority
	}

	return &ConflationUnit{
		priorityQueue:           priorityQueue,
		bundleTypeToPriority:    bundleTypeToPriority,
		readyQueue:              readyQueue,
		bundleMetadataInProcess: nil,
		isInReadyQueue:          false,
		lock:                    sync.Mutex{},
	}
}

// ConflationUnit abstracts the conflation of prioritized multiple bundles with dependencies between them.
type ConflationUnit struct {
	priorityQueue           []*conflationElement
	bundleTypeToPriority    map[string]conflationPriority
	readyQueue              *ConflationReadyQueue
	bundleMetadataInProcess *BundleMetadata
	isInReadyQueue          bool
	lock                    sync.Mutex
}

// insert is an internal function, new bundles are inserted only via conflation manager.
func (cu *ConflationUnit) insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundleType := helpers.GetBundleType(bundle)
	priority := cu.bundleTypeToPriority[bundleType]

	if cu.priorityQueue[priority].bundle != nil &&
		helpers.CompareBundleGenerations(bundle, cu.priorityQueue[priority].bundle) >= 0 {
		return // insert bundle only if the generation we got is newer, otherwise do nothing.
	}

	// if we got here, we got bundle with newer generation
	cu.priorityQueue[priority].bundle = bundle // update the bundle in the priority queue.
	// NOTICE - if the bundle is in process, we replace pointers and not override the values inside the pointers for
	// not changing bundles/metadata that were already given to DB workers for processing.
	incarnation, generation := bundle.GetGeneration()
	if cu.priorityQueue[priority].bundleMetadata != nil && !cu.priorityQueue[priority].isInProcess {
		cu.priorityQueue[priority].bundleMetadata.update(incarnation, generation, metadata)
	} else {
		cu.priorityQueue[priority].bundleMetadata = &BundleMetadata{
			bundleType:              bundleType,
			incarnation:             incarnation,
			generation:              generation,
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
		return nil, nil, nil, errNoReadyBundle // therefore, this shouldn't happen
	}

	conflationElement := cu.priorityQueue[nextBundleToProcessPriority]

	cu.isInReadyQueue = false
	conflationElement.isInProcess = true

	return conflationElement.bundle, conflationElement.bundleMetadata, conflationElement.handlerFunction, nil
}

// ReportResult is used to report the result of bundle handling job.
func (cu *ConflationUnit) ReportResult(metadata *BundleMetadata, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	priority := cu.bundleTypeToPriority[metadata.bundleType] // priority of the bundle that was processed
	conflationElement := cu.priorityQueue[priority]
	conflationElement.isInProcess = false // finished processing bundle

	if err != nil {
		cu.addCUToReadyQueueIfNeeded()
		return
	}
	// otherwise, err is nil, means bundle processing finished successfully
	if helpers.CompareIncarnationGenerationPairs(metadata.incarnation, metadata.generation,
		conflationElement.lastProcessedBundleIncarnation, conflationElement.lastProcessedBundleGeneration) < 0 {
		conflationElement.lastProcessedBundleIncarnation = metadata.incarnation
		conflationElement.lastProcessedBundleGeneration = metadata.generation
	}
	// if bundle wasn't updated since GetNext was called - delete bundle + metadata since it was already processed
	if helpers.CompareIncarnationGenerationPairs(metadata.incarnation, metadata.generation,
		conflationElement.lastProcessedBundleIncarnation, conflationElement.lastProcessedBundleGeneration) == 0 {
		// we won't throw the metadata since the committer may use it
		conflationElement.bundleMetadata.transportBundleMetadata.Processed = true
		conflationElement.bundle = nil
	}

	cu.addCUToReadyQueueIfNeeded()
}

// GetBundlesMetadata provides collections of the CU's bundle transport-metadata.
func (cu *ConflationUnit) GetBundlesMetadata() []*transport.BundleMetadata {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundlesMetadata := make([]*transport.BundleMetadata, 0, len(cu.priorityQueue))

	for _, element := range cu.priorityQueue {
		if element.bundleMetadata == nil {
			continue
		}

		bundlesMetadata = append(bundlesMetadata, &element.bundleMetadata.transportBundleMetadata)
	}

	return bundlesMetadata
}

func (cu *ConflationUnit) isInProcess() bool {
	for _, conflationElement := range cu.priorityQueue {
		if conflationElement.isInProcess {
			return true // if any bundle is in process than conflation unit is in process
		}
	}

	return false
}

func (cu *ConflationUnit) addCUToReadyQueueIfNeeded() {
	if cu.isInReadyQueue || cu.isInProcess() {
		return // allow CU to appear only once in RQ/processing
	}
	// if we reached here, CU is not in RQ nor during processing
	nextReadyBundlePriority := cu.getNextReadyBundlePriority()
	if nextReadyBundlePriority != invalidPriority { // there is a ready to be processed bundle
		cu.readyQueue.Enqueue(cu) // let the dispatcher know this CU has a ready to be processed bundle
		cu.isInReadyQueue = true
	}
}

func (cu *ConflationUnit) getNextReadyBundlePriority() int {
	for priority, conflationElement := range cu.priorityQueue { // going over priority queue according to priorities.
		if conflationElement.bundle != nil && !conflationElement.isInProcess &&
			cu.checkDependencies(conflationElement.bundle) {
			return priority // bundle in this priority is ready to be processed
		} // bundle from this priority exists, we don't have previous generation in processing and dependencies exist
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
	dependencyConflationElement := cu.priorityQueue[dependencyIndex]

	if helpers.CompareIncarnationGenerationPairs(dependency.Incarnation, dependency.Generation,
		dependencyConflationElement.lastProcessedBundleIncarnation,
		dependencyConflationElement.lastProcessedBundleGeneration) < 0 {
		return false // the needed dependency generation wasn't processed yet
	}

	if cu.priorityQueue[dependencyIndex].isInProcess {
		return false // the needed dependency bundle is currently in process, waiting for its processing to finish.
	}

	return true // dependency required generation was processed, and new generation of dependency is not in process
}
