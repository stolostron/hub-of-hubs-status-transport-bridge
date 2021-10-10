package conflator

import (
	"errors"
	"fmt"

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

var (
	errWrongBundleType      = errors.New("received wrong bundle type, expecting DeltaBundle")
	errFailedToUpdateBundle = errors.New("failed to update bundle")
)

// NewBundleInfo returns a new BundleInfo wrapper that handles bundles/metadata according to the given sync mode.
func NewBundleInfo(bundleType string, syncMode SyncMode, initBundleVersion *status.BundleVersion) *BundleInfo {
	bundleInfo := &BundleInfo{
		bundleType:                 bundleType,
		bundle:                     nil,
		metadata:                   nil,
		syncMode:                   syncMode,
		LastProcessedBundleVersion: initBundleVersion,
		LastDispatchedDeltaBundle:  nil,
		bundleUpdaterFunc:          nil,
	}

	// set the updater functions (instead of checking on every call)
	if syncMode == DeltaStateSyncMode {
		bundleInfo.bundleUpdaterFunc = bundleInfo.setDeltaStateBundle
		bundleInfo.metadataUpdaterFunc = bundleInfo.setDeltaStateMetadata
	} else {
		bundleInfo.bundleUpdaterFunc = bundleInfo.setCompleteStateBundle
		bundleInfo.metadataUpdaterFunc = bundleInfo.setCompleteStateMetadata
	}

	return bundleInfo
}

// BundleInfo wraps bundles and their metadata, allowing access to both based on their sync mode.
type BundleInfo struct {
	bundleType string
	bundle     bundle.Bundle
	metadata   *BundleMetadata
	syncMode   SyncMode

	LastProcessedBundleVersion *status.BundleVersion
	LastDispatchedDeltaBundle  bundle.Bundle

	bundleUpdaterFunc   func(bundle.Bundle) (*status.BundleVersion, error)
	metadataUpdaterFunc func(*status.BundleVersion, bundle.Bundle, transport.BundleMetadata)
}

// GetBundle returns the wrapped bundle.
func (bi *BundleInfo) GetBundle() bundle.Bundle {
	return bi.bundle
}

// UpdateInfo updates the wrapped bundle and metadata according to the sync mode.
// createNewObjects boolean sets whether new (bundle/metadata) objects must be pointed to.
func (bi *BundleInfo) UpdateInfo(bundle bundle.Bundle, transportMetadata transport.BundleMetadata,
	createNewObjects bool) error {
	oldBundleVersion, err := bi.bundleUpdaterFunc(bundle)
	if err != nil {
		return fmt.Errorf("%v - %w", errFailedToUpdateBundle, err)
	}

	if bi.metadata == nil {
		bi.metadata = &BundleMetadata{
			bundleType:              bi.bundleType,
			bundleVersion:           bundle.GetVersion(),
			transportBundleMetadata: transportMetadata,
		}

		return nil
	}

	if createNewObjects {
		// create new metadata with identical info and plug it in
		bi.metadata = &BundleMetadata{
			bundleType:              bi.bundleType,
			bundleVersion:           bi.metadata.bundleVersion,
			transportBundleMetadata: bi.metadata.transportBundleMetadata,
		}
	}

	if oldBundleVersion == nil {
		// MarkAsProcessed might have wiped the bundle pointer, replace it with last processed version
		oldBundleVersion = bi.LastProcessedBundleVersion
	}
	// update metadata according to mode.
	bi.metadataUpdaterFunc(oldBundleVersion, bundle, transportMetadata)

	return nil
}

// GetMetadataToDispatch returns the wrapped bundle's metadata for the purpose of dispatching.
// If this bundle is in delta-state sync mode then its content is flushed (a copy is maintained for fail-recovery).
func (bi *BundleInfo) GetMetadataToDispatch() *BundleMetadata {
	if bi.syncMode == DeltaStateSyncMode {
		// save the dispatched bundle content before giving metadata, so that we can start a new line and recover
		// from failure if it happens
		bi.LastDispatchedDeltaBundle = bi.bundle
		bi.bundle = nil
	}

	return bi.metadata
}

// HandleFailure recovers from failure if the bundle is in delta-state sync mode.
// The failed bundle's content is re-merged (not as source of truth) into the current active bundle,
// and the metadata is restored for safe committing.
func (bi *BundleInfo) HandleFailure(failedMetadata *BundleMetadata) {
	if bi.syncMode == DeltaStateSyncMode {
		if bi.bundle == nil || failedMetadata.bundleVersion.Equals(bi.metadata.bundleVersion) {
			// did not receive updates, restore content
			bi.bundle = bi.LastDispatchedDeltaBundle
			bi.metadata = failedMetadata
		} else {
			// failed-metadata bundle version can only be smaller than the current version
			// inherit content of the dispatched bundle
			currentDeltaBundle, _ := bi.bundle.(bundle.DeltaBundle)
			if err := currentDeltaBundle.InheritContent(bi.LastDispatchedDeltaBundle); err != nil {
				// should never happen but just for safety
				return
			}

			// restore transport metadata
			bi.metadata.transportBundleMetadata = failedMetadata.transportBundleMetadata
		}
	}
}

// GetTransportMetadata returns the wrapped bundle's transport metadata.
func (bi *BundleInfo) GetTransportMetadata() transport.BundleMetadata {
	return bi.metadata.transportBundleMetadata
}

// MarkAsProcessed releases the bundle content and marks transport metadata as processed.
func (bi *BundleInfo) MarkAsProcessed(processedMetadata *BundleMetadata) {
	bi.bundle = nil
	if bi.syncMode == CompleteStateSyncMode {
		if processedMetadata.bundleVersion.Equals(bi.metadata.bundleVersion) {
			// if this is the same bundle that was processed then mark it as processed, otherwise leave
			// the current (newer one) as pending.
			bi.metadata.transportBundleMetadata.MarkAsProcessed()
		}
	} else {
		// release the fail-recovery stored bundle pointer
		bi.LastDispatchedDeltaBundle = nil
		if bi.metadata.bundleVersion.NewerThan(processedMetadata.bundleVersion) {
			return // a new line began, this processed delta slipped it-self through a tiny window to a db-syncer
		}
		// the processed one is >= the head of this line, update transport metadata + mark as processed
		bi.metadata.transportBundleMetadata = processedMetadata.transportBundleMetadata
		bi.metadata.transportBundleMetadata.MarkAsProcessed()
	}
}

func (bi *BundleInfo) setCompleteStateBundle(newBundle bundle.Bundle) (*status.BundleVersion, error) {
	// knowing that each bundle contains the full state, switching is as simple as updating the pointer
	bi.bundle = newBundle

	return nil, nil
}

func (bi *BundleInfo) setDeltaStateBundle(newBundle bundle.Bundle) (*status.BundleVersion, error) {
	if bi.bundle == nil {
		bi.bundle = newBundle

		return nil, nil
	}

	oldBundleVersion := bi.bundle.GetVersion()

	newDeltaBundle, ok := newBundle.(bundle.DeltaBundle)
	if !ok {
		return oldBundleVersion, errWrongBundleType
	}

	// update content of newBundle since a delta bundle contains events as opposed to the full-state in-
	// CompleteState bundles.
	if err := newDeltaBundle.InheritContent(bi.bundle); err != nil {
		return oldBundleVersion, fmt.Errorf("%w", err)
	}

	bi.bundle = newBundle

	return oldBundleVersion, nil
}

func (bi *BundleInfo) setCompleteStateMetadata(_ *status.BundleVersion, bundle bundle.Bundle,
	transportMetadata transport.BundleMetadata) {
	bi.metadata.bundleVersion = bundle.GetVersion()
	bi.metadata.transportBundleMetadata = transportMetadata
}

func (bi *BundleInfo) setDeltaStateMetadata(oldBundleVersion *status.BundleVersion, newBundle bundle.Bundle,
	transportMetadata transport.BundleMetadata) {
	bi.metadata.bundleVersion = newBundle.GetVersion()
	// update transport metadata only if bundle starts a new line of deltas
	if newBundle.GetDependency().BundleVersion.NewerThan(oldBundleVersion) {
		bi.metadata.transportBundleMetadata = transportMetadata
	}
}
