package conflator

import (
	"errors"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	bundleinfo "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/bundle-info"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const (
	invalidPriority = -1
)

var (
	errNoReadyBundle               = errors.New("no bundle is ready to be processed")
	errDependencyCannotBeEvaluated = errors.New("bundles declares dependency in registration but doesn't " +
		"implement DependantBundle interface")
)

// ResultReporter is an interface used to report the result of the handler function after its invocation.
// the idea is to have a clear separation of concerns and make sure dispatcher can only request for bundles and
// DB workers can only report results and not request for additional bundles.
// this makes sure DB workers get their input only via the dispatcher which is the entity responsible for reading
// bundles and invoking the handler functions using DB jobs.
// (using this interfaces verifies no developer violates the design that was intended).
type ResultReporter interface {
	ReportResult(metadata *bundleinfo.BundleMetadata, err error)
}

func newConflationUnit(log logr.Logger, readyQueue *ConflationReadyQueue,
	registrations []*ConflationRegistration, requireInitialDependencyChecks bool,
	statistics *statistics.Statistics) *ConflationUnit {
	priorityQueue := make([]*conflationElement, len(registrations))
	bundleTypeToPriority := make(map[string]conflationPriority)

	createBundleInfoFuncMap := map[status.HybridSyncMode]bundleinfo.CreateBundleInfoFunc{
		status.DeltaStateMode:    bundleinfo.NewDeltaStateBundleInfo,
		status.CompleteStateMode: bundleinfo.NewCompleteStateBundleInfo,
	}

	for _, registration := range registrations {
		priorityQueue[registration.priority] = &conflationElement{
			bundleInfo:                 createBundleInfoFuncMap[registration.syncMode](registration.bundleType),
			handlerFunction:            registration.handlerFunction,
			dependency:                 registration.dependency, // nil if there is no dependency
			isInProcess:                false,
			lastProcessedBundleVersion: noBundleVersion(),
		}

		bundleTypeToPriority[registration.bundleType] = registration.priority
	}

	return &ConflationUnit{
		log:                            log,
		priorityQueue:                  priorityQueue,
		bundleTypeToPriority:           bundleTypeToPriority,
		readyQueue:                     readyQueue,
		requireInitialDependencyChecks: requireInitialDependencyChecks,
		isInReadyQueue:                 false,
		lock:                           sync.Mutex{},
		statistics:                     statistics,
	}
}

// ConflationUnit abstracts the conflation of prioritized multiple bundles with dependencies between them.
type ConflationUnit struct {
	log                            logr.Logger
	priorityQueue                  []*conflationElement
	bundleTypeToPriority           map[string]conflationPriority
	readyQueue                     *ConflationReadyQueue
	requireInitialDependencyChecks bool
	isInReadyQueue                 bool
	lock                           sync.Mutex
	statistics                     *statistics.Statistics
}

// insert is an internal function, new bundles are inserted only via conflation manager.
func (cu *ConflationUnit) insert(bundle bundle.Bundle, metadata transport.BundleMetadata) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundleType := helpers.GetBundleType(bundle)
	priority := cu.bundleTypeToPriority[bundleType]
	conflationElement := cu.priorityQueue[priority]
	conflationElementBundle := conflationElement.bundleInfo.GetBundle()

	if !bundle.GetVersion().NewerThan(conflationElement.lastProcessedBundleVersion) {
		return // we got old bundle, a newer (or equal) bundle was already processed.
	}

	if conflationElementBundle != nil && !bundle.GetVersion().NewerThan(conflationElementBundle.GetVersion()) {
		return // insert bundle only if version we got is newer than what we have in memory, otherwise do nothing.
	}

	// start conflation unit metric for specific bundle type - overwrite it each time new bundle arrives
	cu.statistics.StartConflationUnitMetrics(bundle)

	// if we got here, we got bundle with newer version
	// update the bundle in the priority queue.
	if err := conflationElement.bundleInfo.UpdateBundle(bundle); err != nil {
		cu.log.Error(err, "failed to insert bundle")
		return
	}
	// NOTICE - if the bundle is in process, we replace pointers and not override the values inside the pointers for
	// not changing bundles/metadata that were already given to DB workers for processing.
	if conflationElement.bundleInfo.GetMetadata(false) != nil && !conflationElement.isInProcess {
		conflationElement.bundleInfo.UpdateMetadata(bundle.GetVersion(), metadata, false)
		cu.statistics.IncrementNumberOfConflations(bundle)
	} else {
		conflationElement.bundleInfo.UpdateMetadata(bundle.GetVersion(), metadata, true)
	}

	cu.addCUToReadyQueueIfNeeded()
}

// GetNext returns the next ready to be processed bundle and its transport metadata.
func (cu *ConflationUnit) GetNext() (bundle bundle.Bundle, metadata *bundleinfo.BundleMetadata,
	handlerFunc BundleHandlerFunc, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	nextBundleToProcessPriority := cu.getNextReadyBundlePriority()
	if nextBundleToProcessPriority == invalidPriority { // CU adds itself to RQ only when it has ready to process bundle
		return nil, nil, nil, errNoReadyBundle // therefore this shouldn't happen
	}

	conflationElement := cu.priorityQueue[nextBundleToProcessPriority]

	cu.isInReadyQueue = false
	conflationElement.isInProcess = true

	// stop conflation unit metric for specific bundle type - evaluated once bundle is fetched from the priority queue
	cu.statistics.StopConflationUnitMetrics(conflationElement.bundleInfo.GetBundle())

	return conflationElement.bundleInfo.GetBundle(), conflationElement.bundleInfo.GetMetadata(true),
		conflationElement.handlerFunction, nil
}

// ReportResult is used to report the result of bundle handling job.
func (cu *ConflationUnit) ReportResult(metadata *bundleinfo.BundleMetadata, err error) {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	priority := cu.bundleTypeToPriority[metadata.BundleType] // priority of the bundle that was processed
	conflationElement := cu.priorityQueue[priority]
	conflationElement.isInProcess = false // finished processing bundle

	if err != nil {
		if deltaBundleInfo, ok := conflationElement.bundleInfo.(bundleinfo.DeltaBundleInfo); ok {
			deltaBundleInfo.HandleFailure(metadata)
		}

		cu.addCUToReadyQueueIfNeeded()

		return
	}
	// otherwise, err is nil, means bundle processing finished successfully
	if metadata.BundleVersion.NewerThan(conflationElement.lastProcessedBundleVersion) {
		conflationElement.lastProcessedBundleVersion = metadata.BundleVersion
	}
	// if bundle wasn't updated since GetNext was called - mark it as processed (bundle info logic handles releasing
	// data). if bundle is already nil then nothing came in since dispatching to dbsyncer.
	if conflationElement.bundleInfo.GetBundle() == nil ||
		metadata.BundleVersion.Equals(conflationElement.bundleInfo.GetBundle().GetVersion()) {
		conflationElement.bundleInfo.MarkAsProcessed(metadata)
	}

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
		ceBundle := conflationElement.bundleInfo.GetBundle()
		if ceBundle != nil && ceBundle.GetVersion().NewerThan(conflationElement.lastProcessedBundleVersion) &&
			!cu.isCurrentOrAnyDependencyInProcess(conflationElement) &&
			cu.checkDependency(conflationElement) {
			return priority // bundle in this priority is ready to be processed
		}
	}

	return invalidPriority
}

// getBundlesMetadata provides collections of the CU's bundle transport-metadata.
func (cu *ConflationUnit) getBundlesMetadata() []transport.BundleMetadata {
	cu.lock.Lock()
	defer cu.lock.Unlock()

	bundlesMetadata := make([]transport.BundleMetadata, 0, len(cu.priorityQueue))

	for _, element := range cu.priorityQueue {
		if transportMetadata := element.bundleInfo.GetTransportMetadataToCommit(); transportMetadata != nil {
			bundlesMetadata = append(bundlesMetadata, transportMetadata)
		}
	}

	return bundlesMetadata
}

// isCurrentOrAnyDependencyInProcess checks if current element or any dependency from dependency chain is in process.
func (cu *ConflationUnit) isCurrentOrAnyDependencyInProcess(conflationElement *conflationElement) bool {
	if conflationElement.isInProcess { // current conflation element is in process
		return true
	}

	if conflationElement.dependency == nil { // no more dependencies in chain, therefore no dependency in process
		return false
	}

	dependencyIndex := cu.bundleTypeToPriority[conflationElement.dependency.BundleType]

	return cu.isCurrentOrAnyDependencyInProcess(cu.priorityQueue[dependencyIndex])
}

// dependencies are organized in a chain.
func (cu *ConflationUnit) checkDependency(conflationElement *conflationElement) bool {
	if conflationElement.dependency == nil {
		return true // bundle in this conflation element has no dependency
	}

	dependantBundle, ok := conflationElement.bundleInfo.GetBundle().(bundle.DependantBundle)
	if !ok { // this bundle declared it has a dependency but doesn't implement DependantBundle
		cu.log.Error(errDependencyCannotBeEvaluated, "cannot evaluate bundle dependencies, not processing bundle",
			"LeafHubName", conflationElement.bundleInfo.GetBundle().GetLeafHubName(), "BundleType",
			conflationElement.bundleInfo.GetMetadata(false).BundleType)

		return false
	}

	dependencyIndex := cu.bundleTypeToPriority[conflationElement.dependency.BundleType]
	dependencyLastProcessedVersion := cu.priorityQueue[dependencyIndex].lastProcessedBundleVersion

	if !cu.requireInitialDependencyChecks && dependencyLastProcessedVersion.Equals(noBundleVersion()) {
		return true // transport does not require initial dependency check
	}

	switch conflationElement.dependency.DependencyType {
	case dependency.ExactMatch:
		return dependantBundle.GetDependencyVersion().Equals(dependencyLastProcessedVersion)

	case dependency.AtLeast:
		fallthrough // default case is AtLeast

	default:
		return !dependantBundle.GetDependencyVersion().NewerThan(dependencyLastProcessedVersion)
	}
}

func noBundleVersion() *status.BundleVersion {
	return status.NewBundleVersion(0, 0)
}
