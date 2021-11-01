package bundleinfo

import (
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
	GetBundle() bundle.Bundle
	GetBundleType() *string
	GetTransportMetadata() transport.BundleMetadata
	GetMetadataToDispatch() *BundleMetadata
	UpdateInfo(bundle bundle.Bundle, transportMetadata transport.BundleMetadata, createNewObjects bool) error
	HandleFailure(failedMetadata *BundleMetadata)
	MarkAsProcessed(processedMetadata *BundleMetadata)
}

// NewBundleInfo returns a BundleInfo instance based on sync mode.
func NewBundleInfo(bundleType string, syncMode SyncMode) BundleInfo {
	if syncMode == DeltaStateSyncMode {
		return NewDeltaStateBundleInfo(bundleType)
	}

	return NewCompleteStateBundleInfo(bundleType)
}
