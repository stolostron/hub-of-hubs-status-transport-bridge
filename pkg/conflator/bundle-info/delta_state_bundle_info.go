package bundleinfo

import (
	"errors"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

var errWrongBundleType = errors.New("received wrong bundle type, expecting DeltaBundle")

// NewDeltaStateBundleInfo returns a new DeltaStateBundleInfo instance.
func NewDeltaStateBundleInfo(bundleType string) *DeltaStateBundleInfo {
	return &DeltaStateBundleInfo{
		bundle:                        nil,
		metadata:                      nil,
		bundleType:                    bundleType,
		lastDispatchedDeltaBundle:     nil,
		lastReceivedTransportMetadata: nil,
		deltaBundleVersionToRealTransportMetadataMap: make(map[*status.BundleVersion]transport.BundleMetadata),
		currentDeltaLineVersion:                      nil,
		deltaLineHeadBundleVersion:                   nil,
	}
}

// DeltaStateBundleInfo wraps delta-state bundles and their metadata.
type DeltaStateBundleInfo struct {
	bundle   bundle.Bundle
	metadata *BundleMetadata

	bundleType                string
	lastDispatchedDeltaBundle bundle.Bundle

	lastReceivedTransportMetadata                transport.BundleMetadata
	deltaBundleVersionToRealTransportMetadataMap map[*status.BundleVersion]transport.BundleMetadata

	currentDeltaLineVersion    *status.BundleVersion
	deltaLineHeadBundleVersion *status.BundleVersion
}

// GetBundle returns the wrapped bundle.
func (bi *DeltaStateBundleInfo) GetBundle() bundle.Bundle {
	return bi.bundle
}

// GetBundleType returns the bundle type.
func (bi *DeltaStateBundleInfo) GetBundleType() *string {
	return &bi.bundleType
}

// UpdateInfo updates the wrapped bundle and metadata according to the sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *DeltaStateBundleInfo) UpdateInfo(newBundle bundle.Bundle, transportMetadata transport.BundleMetadata,
	createNewObjects bool) error {
	newDeltaBundle, ok := newBundle.(bundle.DeltaBundle)
	if !ok {
		return fmt.Errorf("%w - expecting type %s that implements DeltaBundle", errWrongBundleType, bi.bundleType)
	}

	if bi.currentBundleCanBeInherited() && !bi.bundleStartsNewLine(newDeltaBundle) {
		// update content of newBundle with the currently held info, since a delta bundle contains events as opposed to
		// the full-state in CompleteState bundles.
		if err := newDeltaBundle.InheritContent(bi.bundle); err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	// update metadata
	bi.updateDeltaStateMetadata(newDeltaBundle, createNewObjects, transportMetadata)
	// update bundle
	bi.bundle = newDeltaBundle

	return nil
}

// GetMetadataToDispatch returns the wrapped bundle's metadata for the purpose of dispatching.
func (bi *DeltaStateBundleInfo) GetMetadataToDispatch() *BundleMetadata {
	// save the dispatched bundle content before giving metadata, so that we can start a new line and recover
	// from failure if it happens
	bi.lastDispatchedDeltaBundle = bi.bundle
	// map this dispatched version to the actual offset of the latest participating bundle in accumulation
	// so that it can be committed compactly if allowed
	bi.deltaBundleVersionToRealTransportMetadataMap[bi.metadata.BundleVersion] = bi.lastReceivedTransportMetadata

	return bi.metadata
}

// HandleFailure recovers from failure.
// The failed bundle's content is re-merged (not as source of truth) into the current active bundle,
// and the metadata is restored for safe committing.
func (bi *DeltaStateBundleInfo) HandleFailure(failedMetadata *BundleMetadata) {
	if bi.bundle == nil || failedMetadata.BundleVersion.Equals(bi.metadata.BundleVersion) {
		// did not receive updates, restore content
		bi.bundle = bi.lastDispatchedDeltaBundle
		bi.metadata = failedMetadata
	} else if bi.deltaLineHeadBundleVersion.NewerThan(failedMetadata.BundleVersion) {
		// the failed metadata is from an older delta-line and its version is smaller than current version.
		// inherit content of the dispatched (failed) bundle, since content is flushed upon dispatching.
		currentDeltaBundle, _ := bi.bundle.(bundle.DeltaBundle)
		if err := currentDeltaBundle.InheritContent(bi.lastDispatchedDeltaBundle); err != nil {
			// should never happen but just for safety
			return
		}

		// restore transport metadata
		bi.metadata.TransportBundleMetadata = failedMetadata.TransportBundleMetadata
	}
}

// GetTransportMetadata returns the wrapped bundle's transport metadata.
func (bi *DeltaStateBundleInfo) GetTransportMetadata() transport.BundleMetadata {
	if bi.metadata == nil {
		return nil
	}

	return bi.metadata.TransportBundleMetadata
}

// MarkAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *DeltaStateBundleInfo) MarkAsProcessed(processedMetadata *BundleMetadata) {
	if bi.metadata.BundleVersion.NewerThan(processedMetadata.BundleVersion) {
		return // a new line has begun since the processed bundle was dispatched, don't do anything
	}
	// otherwise, delete pointers to save space
	bi.bundle = nil
	// release the fail-recovery stored bundle pointer
	bi.lastDispatchedDeltaBundle = nil
	// the processed one is >= the head of this line, update transport metadata + mark as processed
	if lastTransportMetadata, found := bi.deltaBundleVersionToRealTransportMetadataMap[processedMetadata.BundleVersion]; found {
		bi.metadata.TransportBundleMetadata = lastTransportMetadata
		delete(bi.deltaBundleVersionToRealTransportMetadataMap, processedMetadata.BundleVersion)
	}

	bi.metadata.TransportBundleMetadata.MarkAsProcessed()
}

func (bi *DeltaStateBundleInfo) updateDeltaStateMetadata(newDeltaBundle bundle.DeltaBundle, createNewObjects bool,
	transportMetadata transport.BundleMetadata) {
	if bi.metadata == nil {
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           newDeltaBundle.GetVersion(),
			TransportBundleMetadata: transportMetadata,
		}
	} else if createNewObjects {
		// create new metadata with identical info and plug it in
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           bi.metadata.BundleVersion,
			TransportBundleMetadata: bi.metadata.TransportBundleMetadata,
		}
	}

	// update version
	bi.metadata.BundleVersion = newDeltaBundle.GetVersion()
	bi.lastReceivedTransportMetadata = transportMetadata

	// update transport metadata only if bundle starts a new line of deltas
	if bi.bundleStartsNewLine(newDeltaBundle) {
		// update current line-version
		bi.currentDeltaLineVersion = newDeltaBundle.GetDependencyVersion()
		bi.deltaLineHeadBundleVersion = bi.metadata.BundleVersion
		bi.metadata.TransportBundleMetadata = transportMetadata
	}
}

func (bi *DeltaStateBundleInfo) bundleStartsNewLine(newDeltaBundle bundle.DeltaBundle) bool {
	return newDeltaBundle.GetDependencyVersion().NewerThan(bi.currentDeltaLineVersion)
}

func (bi *DeltaStateBundleInfo) currentBundleCanBeInherited() bool {
	return bi.bundle != nil && bi.lastDispatchedDeltaBundle != bi.bundle
}
