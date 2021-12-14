package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// newCompleteStateBundleInfo returns a new completeStateBundleInfo instance.
func newCompleteStateBundleInfo() bundleInfo {
	return &completeStateBundleInfo{
		bundle:   nil,
		metadata: nil,
	}
}

// completeStateBundleInfo wraps complete-state bundles and their metadata.
type completeStateBundleInfo struct {
	bundle   bundle.Bundle
	metadata *BundleMetadata
}

// getBundle returns the wrapped bundle.
func (bi *completeStateBundleInfo) getBundle() bundle.Bundle {
	return bi.bundle
}

// getMetadata returns the metadata to be forwarded to processors.
func (bi *completeStateBundleInfo) getMetadata() *BundleMetadata {
	return bi.metadata
}

// updateBundle updates the wrapped bundle and metadata according to the complete-state sync mode.
func (bi *completeStateBundleInfo) updateBundle(bundle bundle.Bundle) error {
	bi.bundle = bundle

	return nil
}

// updateMetadata updates the wrapped metadata according to the complete-state sync mode.
// overwriteObject boolean sets whether to overwrite the current metadata or to create a new instance.
func (bi *completeStateBundleInfo) updateMetadata(bundleType string, version *status.BundleVersion,
	transportMetadata transport.BundleMetadata, overwriteObject bool) {
	if !overwriteObject {
		bi.metadata = &BundleMetadata{
			bundleType:              bundleType,
			bundleVersion:           version,
			transportBundleMetadata: transportMetadata,
		}

		return
	}

	// update metadata
	bi.metadata.bundleVersion = version
	bi.metadata.transportBundleMetadata = transportMetadata
}

// getTransportMetadataToCommit returns the wrapped bundle's transport metadata.
func (bi *completeStateBundleInfo) getTransportMetadataToCommit() transport.BundleMetadata {
	if bi.metadata == nil {
		return nil
	}

	return bi.metadata.transportBundleMetadata
}

// markAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *completeStateBundleInfo) markAsProcessed(processedMetadata *BundleMetadata) {
	if !processedMetadata.bundleVersion.Equals(bi.metadata.bundleVersion) {
		return
	}
	// if this is the same bundle that was processed then mark it as processed, otherwise leave
	// the current (newer one) as pending.
	bi.bundle = nil
	bi.metadata.transportBundleMetadata.MarkAsProcessed()
}
