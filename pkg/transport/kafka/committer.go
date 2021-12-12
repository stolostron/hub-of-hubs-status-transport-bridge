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
			// get metadata (pending and non-pending)
			bundlesMetadata := c.getBundlesMetadataFunc()
			// extract the lowest per partition in the pending bundles, the highest per partition in the
			// processed bundles
			pendingOffsetsToCommit, processedOffsetsToCommit := c.filterMetadataPerPartition(bundlesMetadata)
			// patch the processed offsets map with that of the pending ones, so that if a partition
			// has both types, the pending bundle gains priority (overwrites).
			for partition, offset := range pendingOffsetsToCommit {
				processedOffsetsToCommit[partition] = offset
			}

			if err := c.commitPositions(processedOffsetsToCommit); err != nil {
				c.log.Error(err, "committer failed")
			}
		}
	}
}

func (c *Committer) filterMetadataPerPartition(metadataArray []transport.BundleMetadata) (map[int32]kafka.Offset,
	map[int32]kafka.Offset) {
	// assumes all are in the same topic.
	pendingLowestOffsetsMap := make(map[int32]kafka.Offset)
	processedHighestOffsetsMap := make(map[int32]kafka.Offset)

	for _, transportMetadata := range metadataArray {
		metadata, ok := transportMetadata.(*BundleMetadata)
		if !ok {
			continue // shouldn't happen
		}

		if !metadata.Processed {
			// this belongs to a pending bundle, update the lowest-offsets-map
			lowestOffset, found := pendingLowestOffsetsMap[metadata.partition]
			if found && metadata.offset >= lowestOffset {
				continue // offset is not the lowest in partition, skip
			}

			pendingLowestOffsetsMap[metadata.partition] = metadata.offset
		} else {
			// this belongs to a processed bundle, update the highest-offsets-map
			highestOffset, found := processedHighestOffsetsMap[metadata.partition]
			if found && metadata.offset <= highestOffset {
				continue // offset is not the highest in partition, skip
			}

			processedHighestOffsetsMap[metadata.partition] = metadata.offset
		}
	}

	// increment processed offsets so they are not re-read on kafka consumer restart
	for partition, _ := range processedHighestOffsetsMap {
		processedHighestOffsetsMap[partition]++
	}

	return pendingLowestOffsetsMap, processedHighestOffsetsMap
}

// commitPositions commits the given offsets per partition mapped.
func (c *Committer) commitPositions(offsets map[int32]kafka.Offset) error {
	// go over positions and commit
	for partition, offset := range offsets {
		// skip request if already committed this offset
		if committedOffset, found := c.commitsMap[partition]; found {
			if committedOffset >= offset {
				continue
			}
		}

		topicPartition := kafka.TopicPartition{
			Topic:     &c.topic,
			Partition: partition,
			Offset:    offset,
		}

		if _, err := c.client.Consumer().CommitOffsets([]kafka.TopicPartition{topicPartition}); err != nil {
			return fmt.Errorf("failed to commit offset, stopping bulk commit - %w", err)
		}

		// update commitsMap
		c.commitsMap[partition] = offset

		c.log.Info("committed offset", "topic", c.topic, "partition", partition, "offset", offset)
	}

	return nil
}
