package bundleinfo

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// SyncMode is a type that abstracts the two supported bundle sync modes.
type SyncMode int

const (
	// CompleteStateSyncMode is the mode where each bundle carries the full-state and is independent.
	CompleteStateSyncMode = iota
	// DeltaStateSyncMode is the mode where bundles contain events which translate to partial state.
	DeltaStateSyncMode = iota
)

// BundleInfo abstracts the information/functionality of the two types of bundles (complete/delta state bundles).
type BundleInfo interface {
	// GetBundle returns the bundle.
	GetBundle() bundle.Bundle
	// GetMetadata returns the metadata.
	GetMetadata() *BundleMetadata
	// GetBundleType returns the bundle type.
	GetBundleType() string
	// UpdateBundle updates the bundle according to sync-mode.
	UpdateBundle(bundle bundle.Bundle) error
	// UpdateMetadata updates the metadata according to sync-mode.
	UpdateMetadata(version *status.BundleVersion, transportMetadata transport.BundleMetadata, createNewObject bool)

	// GetTransportMetadataToCommit returns the transport metadata for message committing purposes.
	GetTransportMetadataToCommit() transport.BundleMetadata
	// GetMetadataToDispatch returns the metadata for dispatching purposes.
	GetMetadataToDispatch() *BundleMetadata

	// HandleFailure handles bundle processing failure (data recovery if needed).
	HandleFailure(failedMetadata *BundleMetadata)
	// MarkAsProcessed marks the bundle as processed (propagate to transport.Metadata).
	MarkAsProcessed(processedMetadata *BundleMetadata)
}

// NewBundleInfo returns a BundleInfo instance based on sync mode.
func NewBundleInfo(bundleType string, syncMode SyncMode) BundleInfo {
	if syncMode == DeltaStateSyncMode {
		return NewDeltaStateBundleInfo(bundleType)
	}

	return NewCompleteStateBundleInfo(bundleType)
}
