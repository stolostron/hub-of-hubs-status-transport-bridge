package syncservice

import "github.com/open-horizon/edge-sync-service-client/client"

// BundleMetadata wraps the info required for the associated bundle to be used for marking as consumed.
type BundleMetadata struct {
	processed      bool
	objectMetadata *client.ObjectMetaData
}

// MarkAsProcessed records that the associated bundle has been processed by TransportConsumers.
func (bm *BundleMetadata) MarkAsProcessed() {
	bm.processed = true
}
