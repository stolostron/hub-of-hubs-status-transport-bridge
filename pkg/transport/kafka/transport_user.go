package kafka

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"

// User abstracts the logic required by a transport user, for the Committer to work.
type User interface {
	// GetBundlesMetadata provides collections of PENDING bundle-metadata and NON-PENDING (processed) bundles-metadata.
	GetBundlesMetadata() []*transport.BundleMetadata
}
