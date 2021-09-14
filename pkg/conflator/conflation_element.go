package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

type conflationElement struct {
	bundleType                    string
	bundle                        bundle.Bundle
	bundleMetadata                *BundleMetadata
	handlerFunction               BundleHandlerFunc
	lastProcessedBundleGeneration uint64
}
