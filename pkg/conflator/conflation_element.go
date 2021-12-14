package conflator

import (
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

type conflationElement struct {
	bundleInfo                 bundleInfo
	handlerFunction            BundleHandlerFunc
	dependency                 *dependency.Dependency
	isInProcess                bool
	lastProcessedBundleVersion *status.BundleVersion
}

// update function that updates bundle and metadata and returns whether any error occurred.
func (element *conflationElement) update(bundle bundle.Bundle, metadata transport.BundleMetadata) error {
	if err := element.bundleInfo.updateBundle(bundle); err != nil {
		return fmt.Errorf("failed to update bundle - %w", err)
	}
	// NOTICE - if the bundle is in process, we replace pointers and not override the values inside the pointers for
	// not changing bundles/metadata that were already given to DB workers for processing.
	element.bundleInfo.updateMetadata(bundle.GetVersion(), metadata, !element.isInProcess)

	return nil
}
