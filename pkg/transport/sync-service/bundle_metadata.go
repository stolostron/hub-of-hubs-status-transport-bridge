package syncservice

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

// NewBundleMetadata returns a new instance of BundleMetadata.
func NewBundleMetadata(objectMetadata *client.ObjectMetaData) *BundleMetadata {
	return &BundleMetadata{
		BaseBundleMetadata: transport.BaseBundleMetadata{
			Processed: false,
		},
		objectMetadata: objectMetadata,
	}
}

// BundleMetadata wraps the info required for the associated bundle to be used for marking as consumed.
type BundleMetadata struct {
	transport.BaseBundleMetadata
	objectMetadata *client.ObjectMetaData
}
