package conflator

import (
	"errors"
	"sync"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
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
			bundleType:                       registration.BundleType,
			bundle:                           nil,
			bundleMetadata:                   nil,
			handlerFunction:                  registration.HandlerFunction,
			dependency:                       registration.dependency,
			implicitToExplicitDependencyFunc: registration.implicitToExplicitDependencyFunc,
			isInProcess:                      false,
			lastProcessedBundleGeneration:    bundle.NoGeneration, // no generation was processed yet
		}
		bundleTypeToPriority[registration.BundleType] = registration.Priority
	}

	return &ConflationUnit{
		priorityQueue:        priorityQueue,
		bundleTypeToPriority: bundleTypeToPriority,
		readyQueue:           readyQueue,
		isInReadyQueue:       false,
		lock:                 sync.Mutex{},
	}
}

// ConflationUnit abstracts the conflation of prioritized multiple bundles with dependencies between them.
type ConflationUnit struct {
	priorityQueue        []*conflationElement
	bundleTypeToPriority map[string]conflationPriority
	readyQueue           *ConflationReadyQueue
	isInReadyQueue       bool
	lock                 sync.Mutex
}

// insert is an internal function, new bundles are inserted only via conflation manager.
func (cu *ConflationUnit) insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundleType := helpers.GetBundleType(bundle)
	priority := cu.bundleTypeToPriority[bundleType]

	if bundle.GetGeneration() <= cu.priorityQueue[priority].lastProcessedBundleGeneration {
		return // we got old bundle, a newer (or equal) bundle was already processed.
	}

	if cu.priorityQueue[priority].bundle != nil &&
		bundle.GetGeneration() <= cu.priorityQueue[priority].bundle.GetGeneration() {
		return // insert bundle only if generation we got is newer than what we have in memory, otherwise do nothing.
	}

	dependencyMetadata := dependency.NewDependencyMetadata(cu.priorityQueue[priority].dependency,
		bundle.GetExplicitDependencyGeneration())

	// if we got here, we got bundle with newer generation
	cu.priorityQueue[priority].bundle = bundle // update the bundle in the priority queue.
	// NOTICE - if the bundle is in process, we replace pointers and not override the values inside the pointers
	// for not changing bundles/metadata that were already given to DB workers for processing.
	if cu.priorityQueue[priority].bundleMetadata != nil && !cu.priorityQueue[priority].isInProcess {
		cu.priorityQueue[priority].bundleMetadata.update(bundle.GetGeneration(), dependencyMetadata, metadata)
	} else {
		cu.priorityQueue[priority].bundleMetadata = &BundleMetadata{
			bundleType:              bundleType,
			generation:              bundle.GetGeneration(),
			dependencyMetadata:      dependencyMetadata,
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

	conflationElement := cu.priorityQueue[nextBundleToProcessPriority]

	cu.isInReadyQueue = false
	conflationElement.isInProcess = true

	// if bundle has implicit dependency, put the current generation of dependency in the metadata (for error cases).
	dependencyMetadata := conflationElement.bundleMetadata.dependencyMetadata
	if dependencyMetadata.Dependency != nil && dependencyMetadata.DependencyType == dependency.ImplicitDependency {
		dependencyIndex := cu.bundleTypeToPriority[dependencyMetadata.BundleType]
		dependencyMetadata.Generation = cu.priorityQueue[dependencyIndex].lastProcessedBundleGeneration
	}

	return conflationElement.bundle, conflationElement.bundleMetadata, conflationElement.handlerFunction, nil
}

// ReportResult is used to report the result of bundle handling job.
func (cu *ConflationUnit) ReportResult(metadata *BundleMetadata, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	priority := cu.bundleTypeToPriority[metadata.bundleType] // priority of the bundle that was processed
	cu.priorityQueue[priority].isInProcess = false           // finished processing bundle

	if err != nil {
		cu.updateMetadataAfterError(priority, metadata, err)
		cu.addCUToReadyQueueIfNeeded()

		return
	}
	// otherwise err is nil, means bundle processing finished successfully
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

// returns next ready priority or invalidPriority (-1) in case no priority has a ready to be processed bundle.
func (cu *ConflationUnit) getNextReadyBundlePriority() int {
	for priority, conflationElement := range cu.priorityQueue { // going over priority queue according to priorities.
		if conflationElement.bundle != nil && conflationElement.bundleMetadata != nil &&
			cu.checkDependencies(conflationElement) {
			return priority // bundle in this priority is ready to be processed
		} // bundle from this priority exists, we don't have previous generation in processing and dependencies exist
	}

	return invalidPriority
}

// dependencies are organized in a chain.
// returns true if dependencies current state allows the conflation element to be processed, otherwise returns false.
func (cu *ConflationUnit) checkDependencies(conflationElement *conflationElement) bool {
	if conflationElement.bundleMetadata.dependencyMetadata.Dependency == nil { // conflation element has no dependency
		return true
	}

	if cu.isAnyDependencyInProcess(conflationElement) {
		return false
	}

	// if dependency is explicit, make sure we have the dependency generation that is required
	dependencyMetadata := conflationElement.bundleMetadata.dependencyMetadata
	if dependencyMetadata.Dependency.DependencyType == dependency.ExplicitDependency {
		dependencyIndex := cu.bundleTypeToPriority[dependencyMetadata.Dependency.BundleType]
		if dependencyMetadata.Generation > cu.priorityQueue[dependencyIndex].lastProcessedBundleGeneration {
			return false // the needed dependency generation wasn't processed yet
		}
	}

	return true // dependency required generation was processed, and non of the dependency chain CUEs is in process
}

// isAnyDependencyInProcess checks if current conflation element or any dependency from dependency chain is in process.
func (cu *ConflationUnit) isAnyDependencyInProcess(conflationElement *conflationElement) bool {
	if conflationElement.isInProcess { // current conflation element is in process
		return true
	}

	if conflationElement.dependency == nil { // no more dependencies in chain, therefore no dependency in process
		return false
	}

	dependencyIndex := cu.bundleTypeToPriority[conflationElement.dependency.BundleType]

	return cu.isAnyDependencyInProcess(cu.priorityQueue[dependencyIndex])
}

// updateMetadataAfterError turns implicit dependency into an explicit dependency after error.
func (cu *ConflationUnit) updateMetadataAfterError(priority conflationPriority, metadata *BundleMetadata, err error) {
	if metadata.generation < cu.priorityQueue[priority].bundle.GetGeneration() {
		return // bundle was already replaced with a new one, no need to do anything.
	}

	if metadata.dependencyMetadata.Dependency == nil ||
		metadata.dependencyMetadata.Dependency.DependencyType != dependency.ImplicitDependency {
		return // this function handles at the moment only changing implicit dependency to explicit dependency.
	}

	if cu.priorityQueue[priority].implicitToExplicitDependencyFunc == nil || // verify this function was defined
		!cu.priorityQueue[priority].implicitToExplicitDependencyFunc(err) {
		return // in case the error doesn't not indicate about implicit dependency problem, do nothing.
	}

	// if we got here, the bundle processing failed due to implicit dependency.
	// turn dependency into an explicit dependency, require the dependency to increase at least by one generation.
	metadata.dependencyMetadata = dependency.NewDependencyMetadata(
		dependency.NewDependency(metadata.dependencyMetadata.BundleType, dependency.ExplicitDependency),
		metadata.dependencyMetadata.Generation+1)
}
