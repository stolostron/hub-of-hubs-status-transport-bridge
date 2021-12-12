package kafka

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	kafkaconsumer "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const envVarCommitterInterval = "COMMITTER_INTERVAL"

// CommitPositionsFunc is the function the kafka transport provides for committing positions.
type CommitPositionsFunc func(bundleMetadataMap map[int32]*BundleMetadata) error

// NewCommitter returns a new instance of Committer.
func NewCommitter(log logr.Logger, client *kafkaconsumer.KafkaConsumer,
	getBundlesMetadataFunc transport.GetBundlesMetadataFunc) (*Committer, error) {
	committerIntervalString, found := os.LookupEnv(envVarCommitterInterval)
	if !found {
		return nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarCommitterInterval)
	}

	committerInterval, err := time.ParseDuration(committerIntervalString)
	if err != nil {
		return nil, fmt.Errorf("the environment var %s is not valid duration - %w",
			committerIntervalString, err)
	}

	return &Committer{
		log:                    log,
		client:                 client,
		getBundlesMetadataFunc: getBundlesMetadataFunc,
		commitsMap:             make(map[string]map[int32]kafka.Offset),
		interval:               committerInterval,
		lock:                   sync.Mutex{},
	}, nil
}

// Committer is responsible for committing offsets to transport.
type Committer struct {
	log                    logr.Logger
	client                 *kafkaconsumer.KafkaConsumer
	getBundlesMetadataFunc transport.GetBundlesMetadataFunc
	commitsMap             map[string]map[int32]kafka.Offset
	interval               time.Duration
	lock                   sync.Mutex
}

// Start starts the Committer instance.
func (c *Committer) Start(ctx context.Context) {
	go c.commitOffsets(ctx)
}

func (c *Committer) commitOffsets(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C: // wait for next time interval
			// get metadata (pending, non-pending)
			bundlesMetadata := c.getBundlesMetadataFunc()
			// extract the lowest per partition in the pending bundles, the highest per partition in the
			// processed bundles
			pendingBundleMetadataToCommit,
				processedBundleMetadataToCommit := c.filterMetadataPerPartition(bundlesMetadata)
			// patch the processed bundle-metadata map with that of the pending ones, so that if a partition
			// has both types, the pending bundle gains priority (overwrites).
			for partition, BundleMetadata := range pendingBundleMetadataToCommit {
				processedBundleMetadataToCommit[partition] = BundleMetadata
			}

			if err := c.commitPositions(processedBundleMetadataToCommit); err != nil {
				c.log.Error(err, "committer failed")
			}
		}
	}
}

func (c *Committer) filterMetadataPerPartition(metadataArray []transport.BundleMetadata) (map[int32]*BundleMetadata,
	map[int32]*BundleMetadata) {
	// assumes all are in the same topic, TODO: support multi-topic when needed.
	lowestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)
	highestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)

	for _, transportMetadata := range metadataArray {
		metadata, ok := transportMetadata.(*BundleMetadata)
		if !ok {
			continue // shouldn't happen
		}

		if !metadata.Processed {
			// this belongs to a pending bundle, update the lowest-metadata-map
			if lowestMetadata, found := lowestBundleMetadataPartitionsMap[metadata.topicPartition.Partition]; !found ||
				(found && metadata.topicPartition.Offset < lowestMetadata.topicPartition.Offset) {
				// if no offset was mapped to this partition or if a lower offset is found then update
				lowestBundleMetadataPartitionsMap[metadata.topicPartition.Partition] = metadata
			}
		} else {
			// this belongs to a processed bundle, update the highest-metadata-map
			if highestMetadata,
				found := highestBundleMetadataPartitionsMap[metadata.topicPartition.Partition]; !found ||
				(found && metadata.topicPartition.Offset > highestMetadata.topicPartition.Offset) {
				// if no offset was mapped to this partition or if a higher offset is found then update
				highestBundleMetadataPartitionsMap[metadata.topicPartition.Partition] = metadata
			}
		}
	}

	return lowestBundleMetadataPartitionsMap, highestBundleMetadataPartitionsMap
}

// commitPositions commits the given positions (by metadata) per partition mapped.
func (c *Committer) commitPositions(positions map[int32]*BundleMetadata) error {
	// go over positions and commit
	for _, metadata := range positions {
		// skip request if already committed this data
		if partitionsMap, found := c.commitsMap[*metadata.topicPartition.Topic]; found {
			if committedOffset, found := partitionsMap[metadata.topicPartition.Partition]; found {
				if committedOffset >= metadata.topicPartition.Offset {
					return nil
				}
			}
		}

		// kafka consumer re-reads the latest offset upon starting
		if metadata.Processed {
			metadata.topicPartition.Offset++
		}

		if _, err := c.client.Consumer().CommitOffsets([]kafka.TopicPartition{*metadata.topicPartition}); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}

		// log success and update commitsMap
		c.log.Info("committed offset", "topic", metadata.topicPartition.Topic, "partition",
			metadata.topicPartition.Partition, "offset", metadata.topicPartition.Offset)
		c.updateOffsetsMap(c.commitsMap, metadata.topicPartition.Topic, metadata.topicPartition.Partition,
			metadata.topicPartition.Offset)
	}

	return nil
}

func (c *Committer) updateOffsetsMap(offsetsMap map[string]map[int32]kafka.Offset, topic *string, partition int32,
	offset kafka.Offset) {
	// check if topic is in the map
	if partitionsMap, found := offsetsMap[*topic]; found {
		// check if partition is in topic map
		if offsetInMap, found := partitionsMap[partition]; !found || (found && offsetInMap < offset) {
			// update partition's offset if partition hasn't an offset yet or the new offset is higher.
			partitionsMap[partition] = offset
		}
	} else {
		// create topic and insert pair
		offsetsMap[*topic] = map[int32]kafka.Offset{partition: offset}
	}
}
