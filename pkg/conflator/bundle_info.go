package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// createBundleInfoFunc function that specifies how to create a bundle-info.
type createBundleInfoFunc func() bundleInfo

// bundleInfo abstracts the information/functionality of the two types of bundles (complete/delta state bundles).
type bundleInfo interface {
	// getBundle returns the bundle.
	getBundle() bundle.Bundle
	// getMetadata returns the metadata to forward to processors.
	getMetadata() *BundleMetadata
	// update function to update the bundle and its metadata according to sync-mode.
	update(bundle bundle.Bundle, metadata transport.BundleMetadata, overwriteMetadataObject bool) error
	// getTransportMetadataToCommit returns the transport metadata for message committing purposes.
	getTransportMetadataToCommit() transport.BundleMetadata
	// markAsProcessed marks the bundle as processed.
	markAsProcessed(processedMetadata *BundleMetadata)
}

// deltaBundleInfo extends BundleInfo with delta-bundle related functionalities.
type deltaBundleInfo interface {
	bundleInfo
	// handleFailure handles bundle processing failure (data recovery if needed).
	handleFailure(failedMetadata *BundleMetadata)
}
