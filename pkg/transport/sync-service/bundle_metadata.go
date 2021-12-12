package syncservice

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

// newBundleMetadata returns a new instance of BundleMetadata.
func newBundleMetadata(objectMetadata *client.ObjectMetaData) *bundleMetadata {
	return &bundleMetadata{
		BaseBundleMetadata: transport.BaseBundleMetadata{
			Processed: false,
		},
		objectMetadata: objectMetadata,
	}
}

// bundleMetadata wraps the info required for the associated bundle to be used for marking as consumed.
type bundleMetadata struct {
	transport.BaseBundleMetadata
	objectMetadata *client.ObjectMetaData
}
