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

// NewCommitter returns a new instance of Committer.
func NewCommitter(log logr.Logger, topic string, client *kafkaconsumer.KafkaConsumer,
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
		topic:                  topic,
		client:                 client,
		getBundlesMetadataFunc: getBundlesMetadataFunc,
		commitsMap:             make(map[int32]kafka.Offset),
		interval:               committerInterval,
		lock:                   sync.Mutex{},
	}, nil
}

// Committer is responsible for committing offsets to transport.
type Committer struct {
	log                    logr.Logger
	topic                  string
	client                 *kafkaconsumer.KafkaConsumer
	getBundlesMetadataFunc transport.GetBundlesMetadataFunc
	commitsMap             map[int32]kafka.Offset // map of partition -> offset
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
	// assumes all are in the same topic.
	pendingLowestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)
	processedHighestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)

	for _, transportMetadata := range metadataArray {
		metadata, ok := transportMetadata.(*BundleMetadata)
		if !ok {
			continue // shouldn't happen
		}

		if !metadata.Processed {
			// this belongs to a pending bundle, update the lowest-metadata-map
			lowestMetadata, found := pendingLowestBundleMetadataPartitionsMap[metadata.partition]
			if found && metadata.offset >= lowestMetadata.offset {
				continue // already committed a >= offset
			}

			pendingLowestBundleMetadataPartitionsMap[metadata.partition] = metadata
		} else {
			// this belongs to a processed bundle, update the highest-metadata-map
			highestMetadata, found := processedHighestBundleMetadataPartitionsMap[metadata.partition]
			if found && metadata.offset <= highestMetadata.offset {
				continue // already committed a >= offset
			}

			processedHighestBundleMetadataPartitionsMap[metadata.partition] = metadata
		}
	}

	return pendingLowestBundleMetadataPartitionsMap, processedHighestBundleMetadataPartitionsMap
}

// commitPositions commits the given positions (by metadata) per partition mapped.
func (c *Committer) commitPositions(positions map[int32]*BundleMetadata) error {
	// go over positions and commit
	for _, metadata := range positions { // each metadata corresponds to a single partition
		// skip request if already committed this data
		if committedOffset, found := c.commitsMap[metadata.partition]; found {
			if committedOffset >= metadata.offset {
				return nil
			}
		}

		topicPartition := kafka.TopicPartition{
			Topic:     &c.topic,
			Partition: metadata.partition,
			Offset:    metadata.offset,
		}

		// kafka consumer re-reads the latest offset upon starting, increment if bundle is processed
		if metadata.Processed {
			topicPartition.Offset++
		}

		if _, err := c.client.Consumer().CommitOffsets([]kafka.TopicPartition{topicPartition}); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}

		// log success and update commitsMap
		c.log.Info("committed offset", "topic", c.topic, "partition",
			metadata.partition, "offset", metadata.offset)
		c.updateCommitsMap(&topicPartition)
	}

	return nil
}

func (c *Committer) updateCommitsMap(metadata *kafka.TopicPartition) {
	// check if partition is in map
	offsetInMap, found := c.commitsMap[metadata.Partition]
	if found && offsetInMap >= metadata.Offset {
		return
	}

	// update partition's offset if partition hasn't an offset yet or the new offset is higher.
	c.commitsMap[metadata.Partition] = metadata.Offset
}
