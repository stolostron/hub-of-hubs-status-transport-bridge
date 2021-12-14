package conflator

import (
	"errors"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

var errWrongBundleType = errors.New("received wrong bundle type, expecting DeltaStateBundle")

// newDeltaStateBundleInfo returns a new DeltaStateBundleInfo instance.
func newDeltaStateBundleInfo(bundleType string) bundleInfo {
	return &deltaStateBundleInfo{
		bundle:     nil,
		metadata:   nil,
		bundleType: bundleType,
		lastDispatchedDeltaBundleData: recoverableDeltaStateBundleData{
			bundle:            nil,
			transportMetadata: nil,
		},
		lastReceivedTransportMetadata: nil,
		currentDeltaLineVersion:       nil,
		deltaLineHeadBundleVersion:    nil,
	}
}

// deltaStateBundleInfo wraps delta-state bundles and their metadata.
type deltaStateBundleInfo struct {
	bundle   bundle.DeltaStateBundle
	metadata *BundleMetadata

	bundleType                    string
	lastDispatchedDeltaBundleData recoverableDeltaStateBundleData
	lastReceivedTransportMetadata transport.BundleMetadata

	currentDeltaLineVersion    *status.BundleVersion
	deltaLineHeadBundleVersion *status.BundleVersion
}

type recoverableDeltaStateBundleData struct {
	bundle            bundle.DeltaStateBundle
	transportMetadata transport.BundleMetadata
}

// getBundle returns the wrapped bundle.
func (bi *deltaStateBundleInfo) getBundle() bundle.Bundle {
	return bi.bundle
}

// getMetadata returns the metadata to be forwarded to processors.
func (bi *deltaStateBundleInfo) getMetadata() *BundleMetadata {
	// save the dispatched bundle content before giving metadata, so that we can start a new line and recover
	// from failure if it happens
	bi.lastDispatchedDeltaBundleData.bundle = bi.bundle
	// lastDispatchedDeltaBundleData.transportMetadata will be used later in case the processing fails, therefore
	// it should point to the delta-bundle's earliest pending contributor's (current transport metadata)
	bi.lastDispatchedDeltaBundleData.transportMetadata = bi.metadata.transportBundleMetadata
	// the bundle's transport metadata will be used to mark as processed, therefore we should give out that of the
	// latest pending contributing delta-bundle's
	bi.metadata.transportBundleMetadata = bi.lastReceivedTransportMetadata
	// reset bundle only when dispatching, to start a new delta-pack
	bi.bundle = nil

	return bi.metadata
}

// updateBundle updates the wrapped bundle and metadata according to the sync mode.
func (bi *deltaStateBundleInfo) updateBundle(newBundle bundle.Bundle) error {
	newDeltaBundle, ok := newBundle.(bundle.DeltaStateBundle)
	if !ok {
		return fmt.Errorf("%w - received type %s", errWrongBundleType, bi.bundleType)
	}

	if bi.bundle != nil && !bi.bundleStartsNewLine(newDeltaBundle) {
		// update content of newBundle with the currently held info, since a delta bundle contains events as opposed to
		// the full-state in CompleteState bundles.
		if err := newDeltaBundle.InheritEvents(bi.bundle); err != nil {
			return fmt.Errorf("%w", err)
		}
	}
	// update bundle
	bi.bundle = newDeltaBundle

	return nil
}

// updateMetadata updates the wrapped metadata according to the delta-state sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *deltaStateBundleInfo) updateMetadata(version *status.BundleVersion,
	transportMetadata transport.BundleMetadata, overwriteObject bool) {
	if bi.metadata == nil { // new metadata
		bi.metadata = &BundleMetadata{
			bundleType:              bi.bundleType,
			bundleVersion:           version,
			transportBundleMetadata: transportMetadata,
		}
	} else if !overwriteObject {
		// create new metadata with identical info and plug it in
		bi.metadata = &BundleMetadata{
			bundleType:              bi.bundleType,
			bundleVersion:           bi.metadata.bundleVersion,
			transportBundleMetadata: bi.metadata.transportBundleMetadata, // preserve metadata of the earliest
		}
	}
	// update version
	bi.metadata.bundleVersion = version
	// update latest received transport metadata for committing later if needed
	bi.lastReceivedTransportMetadata = transportMetadata

	// update transport metadata only if bundle starts a new line of deltas
	if bi.bundleStartsNewLine(bi.bundle) {
		// update current line-version
		bi.currentDeltaLineVersion = bi.bundle.GetDependencyVersion()
		bi.deltaLineHeadBundleVersion = bi.metadata.bundleVersion
		bi.metadata.transportBundleMetadata = transportMetadata
	}
}

// handleFailure recovers from failure.
// The failed bundle's content is re-merged (not as source of truth) into the current active bundle,
// and the metadata is restored for safe committing (back to the first merged pending delta bundle's).
func (bi *deltaStateBundleInfo) handleFailure(failedMetadata *BundleMetadata) {
	lastDispatchedDeltaBundle := bi.lastDispatchedDeltaBundleData.bundle
	lastDispatchedTransportMetadata := bi.lastDispatchedDeltaBundleData.transportMetadata
	// release currently saved data
	bi.lastDispatchedDeltaBundleData.bundle = nil
	bi.lastDispatchedDeltaBundleData.transportMetadata = nil

	if bi.deltaLineHeadBundleVersion.NewerThan(failedMetadata.bundleVersion) {
		return // failed bundle's content is irrelevant since a covering baseline was received
	} else if bi.bundle == nil {
		// did not receive updates, restore content
		bi.bundle = bi.lastDispatchedDeltaBundleData.bundle
	} else if err := bi.bundle.InheritEvents(lastDispatchedDeltaBundle); err != nil {
		// otherwise, the failed metadata is NOT from an older delta-line and its version is smaller than current-
		// version (since bi.bundle is not nil, therefore a bundle got in after the last dispatch for sure).
		// inherit content of the dispatched (failed) bundle, since content is flushed upon dispatching.
		//  -- the error should never happen but just for safety
		return
	}
	// restore metadata
	bi.metadata = failedMetadata
	// restore transport metadata to that of the earliest contributor in the saved delta-pack
	bi.metadata.transportBundleMetadata = lastDispatchedTransportMetadata
}

// getTransportMetadataToCommit returns the wrapped bundle's transport metadata.
func (bi *deltaStateBundleInfo) getTransportMetadataToCommit() transport.BundleMetadata {
	if bi.metadata == nil {
		return nil
	}

	return bi.metadata.transportBundleMetadata
}

// markAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *deltaStateBundleInfo) markAsProcessed(processedMetadata *BundleMetadata) {
	if bi.metadata.bundleVersion.NewerThan(processedMetadata.bundleVersion) {
		// release fail-recovery data
		bi.lastDispatchedDeltaBundleData.bundle = nil
		bi.lastDispatchedDeltaBundleData.transportMetadata = nil

		return // a new delta has begun since the processed bundle was dispatched, don't do anything
	}
	// no new delta started, mark the latest dispatched bundle metadata as processed and make it active
	// note: the transport metadata of non-processed delta bundles is that of the earliest (in pending state), therefore
	// we need to swap it with that of the last dispatched.
	bi.metadata.transportBundleMetadata = bi.lastDispatchedDeltaBundleData.transportMetadata
	bi.metadata.transportBundleMetadata.MarkAsProcessed()
	// release fail-recovery data
	bi.lastDispatchedDeltaBundleData.bundle = nil
	bi.lastDispatchedDeltaBundleData.transportMetadata = nil
}

func (bi *deltaStateBundleInfo) bundleStartsNewLine(newDeltaBundle bundle.DeltaStateBundle) bool {
	return newDeltaBundle.GetDependencyVersion().NewerThan(bi.currentDeltaLineVersion)
}
