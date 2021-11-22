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
		bundle:     nil,
		metadata:   nil,
		bundleType: bundleType,
		lastDispatchedDeltaBundleData: RecoverableDeltaStateBundleData{
			bundle:            nil,
			transportMetadata: nil,
		},
		lastReceivedTransportMetadata: nil,
		currentDeltaLineVersion:       nil,
		deltaLineHeadBundleVersion:    nil,
	}
}

// DeltaStateBundleInfo wraps delta-state bundles and their metadata.
type DeltaStateBundleInfo struct {
	bundle   bundle.DeltaBundle
	metadata *BundleMetadata

	bundleType                    string
	lastDispatchedDeltaBundleData RecoverableDeltaStateBundleData
	lastReceivedTransportMetadata transport.BundleMetadata

	currentDeltaLineVersion    *status.BundleVersion
	deltaLineHeadBundleVersion *status.BundleVersion
}

type RecoverableDeltaStateBundleData struct {
	bundle            bundle.DeltaBundle
	transportMetadata transport.BundleMetadata
}

// GetBundle returns the wrapped bundle.
func (bi *DeltaStateBundleInfo) GetBundle() bundle.Bundle {
	return bi.bundle
}

// GetMetadata returns the metadata.
func (bi *DeltaStateBundleInfo) GetMetadata() *BundleMetadata {
	return bi.metadata
}

// GetBundleType returns the bundle type.
func (bi *DeltaStateBundleInfo) GetBundleType() string {
	return bi.bundleType
}

// UpdateBundle updates the wrapped bundle and metadata according to the sync mode.
func (bi *DeltaStateBundleInfo) UpdateBundle(newBundle bundle.Bundle) error {
	newDeltaBundle, ok := newBundle.(bundle.DeltaBundle)
	if !ok {
		return fmt.Errorf("%w - expecting type %s that implements DeltaBundle", errWrongBundleType, bi.bundleType)
	}

	if bi.bundle != nil && !bi.bundleStartsNewLine(newDeltaBundle) {
		// update content of newBundle with the currently held info, since a delta bundle contains events as opposed to
		// the full-state in CompleteState bundles.
		if err := newDeltaBundle.InheritContent(bi.bundle); err != nil {
			return fmt.Errorf("%w", err)
		}
	}
	// update bundle
	bi.bundle = newDeltaBundle

	return nil
}

// UpdateMetadata updates the wrapped metadata according to the delta-state sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *DeltaStateBundleInfo) UpdateMetadata(version *status.BundleVersion,
	transportMetadata transport.BundleMetadata, createNewObject bool) {
	if bi.metadata == nil {
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           version,
			TransportBundleMetadata: transportMetadata,
		}
	} else if createNewObject {
		// create new metadata with identical info and plug it in
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           bi.metadata.BundleVersion,
			TransportBundleMetadata: bi.metadata.TransportBundleMetadata,
		}
	}
	// update version
	bi.metadata.BundleVersion = version
	// update latest transport metadata for committing later if needed
	bi.lastReceivedTransportMetadata = transportMetadata

	// update transport metadata only if bundle starts a new line of deltas
	if bi.bundleStartsNewLine(bi.bundle) {
		// update current line-version
		bi.currentDeltaLineVersion = bi.bundle.GetDependencyVersion()
		bi.deltaLineHeadBundleVersion = bi.metadata.BundleVersion
		bi.metadata.TransportBundleMetadata = transportMetadata
	}
}

// GetMetadataToDispatch returns the wrapped bundle's metadata for the purpose of dispatching.
func (bi *DeltaStateBundleInfo) GetMetadataToDispatch() *BundleMetadata {
	// save the dispatched bundle content before giving metadata, so that we can start a new line and recover
	// from failure if it happens
	bi.lastDispatchedDeltaBundleData.bundle = bi.bundle
	bi.lastDispatchedDeltaBundleData.transportMetadata = bi.metadata.TransportBundleMetadata

	bi.bundle = nil

	return bi.metadata
}

// HandleFailure recovers from failure.
// The failed bundle's content is re-merged (not as source of truth) into the current active bundle,
// and the metadata is restored for safe committing.
func (bi *DeltaStateBundleInfo) HandleFailure(failedMetadata *BundleMetadata) {
	if bi.bundle == nil {
		// did not receive updates, restore content
		bi.bundle = bi.lastDispatchedDeltaBundleData.bundle
		bi.metadata = failedMetadata
	} else if bi.deltaLineHeadBundleVersion.NewerThan(failedMetadata.BundleVersion) {
		// the failed metadata is from an older delta-line and its version is smaller than current version.
		// inherit content of the dispatched (failed) bundle, since content is flushed upon dispatching.
		if err := bi.bundle.InheritContent(bi.lastDispatchedDeltaBundleData.bundle); err != nil {
			// should never happen but just for safety
			return
		}

		// restore transport metadata
		bi.metadata.TransportBundleMetadata = failedMetadata.TransportBundleMetadata
	}
	bi.lastDispatchedDeltaBundleData.bundle = nil
	bi.lastDispatchedDeltaBundleData.transportMetadata = nil
}

// GetTransportMetadataToCommit returns the wrapped bundle's transport metadata.
func (bi *DeltaStateBundleInfo) GetTransportMetadataToCommit() transport.BundleMetadata {
	if bi.metadata == nil {
		return nil
	}

	return bi.metadata.TransportBundleMetadata
}

// MarkAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *DeltaStateBundleInfo) MarkAsProcessed(processedMetadata *BundleMetadata) {
	if bi.metadata.BundleVersion.NewerThan(processedMetadata.BundleVersion) {
		// release fail-recovery data
		bi.lastDispatchedDeltaBundleData.bundle = nil
		bi.lastDispatchedDeltaBundleData.transportMetadata = nil
		return // a new line has begun since the processed bundle was dispatched, don't do anything
	}
	// no new line started, mark the latest dispatched bundle metadata as processed and make it active
	bi.metadata.TransportBundleMetadata = bi.lastDispatchedDeltaBundleData.transportMetadata
	bi.metadata.TransportBundleMetadata.MarkAsProcessed()
	// release fail-recovery data
	bi.lastDispatchedDeltaBundleData.bundle = nil
	bi.lastDispatchedDeltaBundleData.transportMetadata = nil
}

func (bi *DeltaStateBundleInfo) bundleStartsNewLine(newDeltaBundle bundle.DeltaBundle) bool {
	return newDeltaBundle.GetDependencyVersion().NewerThan(bi.currentDeltaLineVersion)
}
