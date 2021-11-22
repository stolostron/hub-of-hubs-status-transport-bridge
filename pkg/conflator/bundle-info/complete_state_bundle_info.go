package bundleinfo

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
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

// GetMetadata returns the metadata.
func (bi *CompleteStateBundleInfo) GetMetadata() *BundleMetadata {
	return bi.metadata
}

// GetBundleType returns the bundle type.
func (bi *CompleteStateBundleInfo) GetBundleType() string {
	return bi.bundleType
}

// UpdateBundle updates the wrapped bundle and metadata according to the sync mode. o.
func (bi *CompleteStateBundleInfo) UpdateBundle(bundle bundle.Bundle) error {
	bi.bundle = bundle

	return nil
}

// UpdateMetadata updates the wrapped metadata according to the complete-state sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *CompleteStateBundleInfo) UpdateMetadata(version *status.BundleVersion,
	transportMetadata transport.BundleMetadata, createNewObject bool) {
	if createNewObject {
		bi.metadata = &BundleMetadata{
			BundleType:              bi.bundleType,
			BundleVersion:           version,
			TransportBundleMetadata: transportMetadata,
		}

		return
	}

	// update metadata
	bi.metadata.BundleVersion = version
	bi.metadata.TransportBundleMetadata = transportMetadata
}

// GetMetadataToDispatch returns the wrapped bundle's metadata for the purpose of dispatching.
func (bi *CompleteStateBundleInfo) GetMetadataToDispatch() *BundleMetadata {
	return bi.metadata
}

// HandleFailure recovers from failure (does nothing, CEs would have the latest state anyway).
func (bi *CompleteStateBundleInfo) HandleFailure(failedMetadata *BundleMetadata) {}

// GetTransportMetadataToCommit returns the wrapped bundle's transport metadata.
func (bi *CompleteStateBundleInfo) GetTransportMetadataToCommit() transport.BundleMetadata {
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
