package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
)

type conflationElement struct {
	bundleType                 string
	bundle                     bundle.Bundle
	bundleMetadata             *BundleMetadata
	handlerFunction            BundleHandlerFunc
	dependency                 *dependency.Dependency
	isInProcess                bool
	lastProcessedBundleVersion *status.BundleVersion
}

