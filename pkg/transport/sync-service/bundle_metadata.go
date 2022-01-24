package syncservice

import (
	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// newBundleMetadata returns a new instance of BundleMetadata.
func newBundleMetadata(objectMetadata *client.ObjectMetaData) *bundleMetadata {
	return &bundleMetadata{
		BaseBundleMetadata: transport.NewBaseBundleMetadata(),
		objectMetadata:     objectMetadata,
	}
}

// bundleMetadata wraps the info required for the associated bundle to be used for marking as consumed.
type bundleMetadata struct {
	*transport.BaseBundleMetadata
	objectMetadata *client.ObjectMetaData
}
