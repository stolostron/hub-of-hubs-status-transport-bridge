package syncservice

// BundleMetadata wraps the info required for the associated bundle to be used for marking as consumed.
type BundleMetadata struct{}

// MarkAsProcessed records that the associated bundle has been processed by TransportConsumers.
func (bm *BundleMetadata) MarkAsProcessed() {}
