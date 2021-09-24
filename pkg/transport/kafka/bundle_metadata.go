package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// BundleMetadata wraps the info required for the associated bundle to be used for committing purposes.
type BundleMetadata struct {
	Processed bool
	Partition int32
	Offset    kafka.Offset
	Topic     *string
}

// MarkAsProcessed records that the associated bundle has been processed by TransportConsumers.
func (bm *BundleMetadata) MarkAsProcessed() {
	bm.Processed = true
}
