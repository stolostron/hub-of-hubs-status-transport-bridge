package bundleinfo

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewCompleteStateBundleInfo returns a new DeltaStateBundleInfo instance.
func NewCompleteStateBundleInfo(bundleType string) *CompleteStateBundleInfo {
	return &CompleteStateBundleInfo{
		bundle:     nil,
		metadata:   nil,
		bundleType: bundleType,
	}
}

// CompleteStateBundleInfo wraps complete-state bundles and their metadata.
type CompleteStateBundleInfo struct {
	bundle     bundle.Bundle
	metadata   *BundleMetadata
	bundleType string
}

// GetBundle returns the wrapped bundle.
func (bi *CompleteStateBundleInfo) GetBundle() bundle.Bundle {
	return bi.bundle
}

// GetBundleType returns the bundle type.
func (bi *CompleteStateBundleInfo) GetBundleType() *string {
	return &bi.bundleType
}

// UpdateInfo updates the wrapped bundle and metadata according to the sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *CompleteStateBundleInfo) UpdateInfo(bundle bundle.Bundle, transportMetadata transport.BundleMetadata,
	createNewObjects bool) error {
	bi.bundle = bundle

	if createNewObjects || bi.metadata == nil {
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           bundle.GetVersion(),
			TransportBundleMetadata: transportMetadata,
		}

		return nil
	}

	// update metadata
	bi.metadata.BundleVersion = bundle.GetVersion()
	bi.metadata.TransportBundleMetadata = transportMetadata

	return nil
}

// GetMetadataToDispatch returns the wrapped bundle's metadata for the purpose of dispatching.
// If this bundle is in delta-state sync mode then its content is flushed (a copy is maintained for fail-recovery).
func (bi *CompleteStateBundleInfo) GetMetadataToDispatch() *BundleMetadata {
	return bi.metadata
}

// HandleFailure recovers from failure (does nothing, CEs would have the latest state anyway).
func (bi *CompleteStateBundleInfo) HandleFailure(failedMetadata *BundleMetadata) {}

// GetTransportMetadata returns the wrapped bundle's transport metadata.
func (bi *CompleteStateBundleInfo) GetTransportMetadata() transport.BundleMetadata {
	if bi.metadata == nil {
		return nil
	}

	return bi.metadata.TransportBundleMetadata
}

// MarkAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *CompleteStateBundleInfo) MarkAsProcessed(processedMetadata *BundleMetadata) {
	if processedMetadata.BundleVersion.Equals(bi.metadata.BundleVersion) {
		bi.bundle = nil
		// if this is the same bundle that was processed then mark it as processed, otherwise leave
		// the current (newer one) as pending.
		bi.metadata.TransportBundleMetadata.MarkAsProcessed()
	}
}
