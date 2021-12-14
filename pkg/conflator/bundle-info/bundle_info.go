package bundleinfo

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewBundleInfo returns a BundleInfo instance based on sync mode.
func NewBundleInfo(bundleType string, syncMode status.HybridSyncMode) BundleInfo {
	if syncMode == status.DeltaStateMode {
		return NewDeltaStateBundleInfo(bundleType)
	}

	return NewCompleteStateBundleInfo(bundleType)
}

// BundleInfo abstracts the information/functionality of the two types of bundles (complete/delta state bundles).
type BundleInfo interface {
	// GetBundle returns the bundle.
	GetBundle() bundle.Bundle
	// GetMetadata returns the metadata. If the call is to dispatch the metadata then toDispatch must be set to true.
	GetMetadata(toDispatch bool) *BundleMetadata
	// UpdateBundle updates the bundle according to sync-mode.
	UpdateBundle(bundle bundle.Bundle) error
	// UpdateMetadata updates the metadata according to sync-mode.
	UpdateMetadata(version *status.BundleVersion, transportMetadata transport.BundleMetadata, createNewObject bool)

	// GetTransportMetadataToCommit returns the transport metadata for message committing purposes.
	GetTransportMetadataToCommit() transport.BundleMetadata
	// MarkAsProcessed marks the bundle as processed (propagate to transport.Metadata).
	MarkAsProcessed(processedMetadata *BundleMetadata)
}

// HybridBundleInfo extends BundleInfo with hybrid-bundle related functionalities.
type HybridBundleInfo interface {
	BundleInfo
	// HandleFailure handles bundle processing failure (data recovery if needed).
	HandleFailure(failedMetadata *BundleMetadata)
}
