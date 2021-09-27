package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// BundleMetadata abstracts metadata of conflation elements inside the conflation units.
type BundleMetadata struct {
	bundleType string
	generation uint64
	// dependency metadata is information we need for handling implicit/explicit dependencies. a dependency might turn
	// from an implicit dependency to an explicit dependency under certain conditions.
	dependencyMetadata *dependency.Metadata
	// transport metadata is information we need for marking bundle as consumed in transport (e.g. commit offset)
	transportBundleMetadata transport.BundleMetadata
}

func (metadata *BundleMetadata) update(generation uint64, dependencyMetadata *dependency.Metadata,
	transportBundleMetadata transport.BundleMetadata) {
	metadata.generation = generation
	metadata.dependencyMetadata = dependencyMetadata
	metadata.transportBundleMetadata = transportBundleMetadata
}
