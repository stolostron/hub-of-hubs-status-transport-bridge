package conflator

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"

// BundleMetadata abstracts metadata of conflation elements inside the conflation units.
type BundleMetadata struct {
	bundleType string
	generation uint64
	// transport metadata is information we need for marking bundle as consumed in transport (e.g. commit offset)
	transportBundleMetadata transport.BundleMetadata
}

func (metadata *BundleMetadata) update(generation uint64, transportBundleMetadata transport.BundleMetadata) {
	metadata.generation = generation
	metadata.transportBundleMetadata = transportBundleMetadata
}
