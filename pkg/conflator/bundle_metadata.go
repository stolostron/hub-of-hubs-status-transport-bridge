package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// BundleMetadata abstracts metadata of conflation elements inside the conflation units.
type BundleMetadata struct {
	bundleType  string
	incarnation uint64
	generation  uint64
	// transport metadata is information we need for marking bundle as consumed in transport (e.g. commit offset)
	transportBundleMetadata transport.BundleMetadata
}

func (metadata *BundleMetadata) update(incarnation, generation uint64, transportMetadata transport.BundleMetadata) {
	metadata.incarnation = incarnation
	metadata.generation = generation
	metadata.transportBundleMetadata = transportMetadata
}
