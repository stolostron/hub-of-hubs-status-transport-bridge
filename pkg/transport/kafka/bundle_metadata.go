package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewBundleMetadata returns a new instance of BundleMetadata.
func NewBundleMetadata(partition int32, offset kafka.Offset) *BundleMetadata {
	return &BundleMetadata{
		BaseBundleMetadata: transport.BaseBundleMetadata{
			Processed: false,
		},
		partition: partition,
		offset:    offset,
	}
}

// BundleMetadata wraps the info required for the associated bundle to be used for committing purposes.
type BundleMetadata struct {
	transport.BaseBundleMetadata
	partition int32
	offset    kafka.Offset
}
