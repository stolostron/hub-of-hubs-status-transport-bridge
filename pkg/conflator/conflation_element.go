package conflator

import (
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
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
	element.bundleInfo.updateMetadata(helpers.GetBundleType(bundle), bundle.GetVersion(), metadata,
		!element.isInProcess)

	return nil
}

// getBundleForProcessing function to return Bundle and BundleMetadata to forward to processors.
// At the end of this call, the bundle may be released (set to nil).
func (element *conflationElement) getBundleForProcessing() (bundle.Bundle, *BundleMetadata) {
	// getBundle must be called before getMetadata since getMetadata assumes that the bundle is being forwarded
	// to processors, therefore it may release the bundle (set to nil) and apply other dispatch-related functionality.
	return element.bundleInfo.getBundle(), element.bundleInfo.getMetadata()
}
