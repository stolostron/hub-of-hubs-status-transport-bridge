package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// BundleMetadata abstracts metadata of conflation elements inside the conflation units.
type BundleMetadata struct {
	bundleType string
	version    *status.BundleVersion
	// transport metadata is information we need for marking bundle as consumed in transport (e.g. commit offset)
	transportBundleMetadata transport.BundleMetadata
}

func (metadata *BundleMetadata) update(version *status.BundleVersion,
	transportBundleMetadata transport.BundleMetadata) {
	metadata.version = version
	metadata.transportBundleMetadata = transportBundleMetadata
}
