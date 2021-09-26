package conflator

import (
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

type conflationElement struct {
	bundleType                 string
	bundle                     bundle.Bundle
	bundleMetadata             *BundleMetadata
	handlerFunction            BundleHandlerFunc
	isInProcess                bool
	lastProcessedBundleVersion *statusbundle.BundleVersion
}
