package kafka

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const envVarCommitterInterval = "COMMITTER_INTERVAL"

// CommitPositionsFunc is the function the kafka transport provides for committing positions.
type CommitPositionsFunc func(bundleMetadataMap map[int32]*BundleMetadata) error

// NewCommitter returns a new instance of Committer.
func NewCommitter(log logr.Logger, commitPositionsFunc CommitPositionsFunc) (*Committer, error) {
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
		log:                 log,
		commitPositionsFunc: commitPositionsFunc,
		consumers:           make(map[transport.Consumer]struct{}),
		interval:            committerInterval,
		lock:                sync.Mutex{},
	}, nil
}

// Committer is responsible for committing offsets to transport.
type Committer struct {
	log                 logr.Logger
	commitPositionsFunc CommitPositionsFunc
	consumers           map[transport.Consumer]struct{}
	interval            time.Duration
	lock                sync.Mutex
}

// Start starts the Committer instance.
func (c *Committer) Start(ctx context.Context) {
	go c.commitOffsets(ctx)
}

// AddTransportConsumer adds a transport-Consumer that the committer will refer to when looking for offsets to commit.
func (c *Committer) AddTransportConsumer(user transport.Consumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, found := c.consumers[user]; !found {
		c.consumers[user] = struct{}{}
	}
}

func (c *Committer) commitOffsets(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C: // wait for next time interval
			pendingBundleMetadataToCommit := make(map[int32]*BundleMetadata)
			processedBundleMetadataToCommit := make(map[int32]*BundleMetadata)

			// fill map with the lowest offsets per partition via iterating over all registered consumers
			for Consumer := range c.consumers {
				// get metadata (pending, non-pending) from Consumer
				bundlesMetadata := Consumer.GetBundlesMetadata()
				// extract the lowest per partition in the pending bundles, the highest per partition in the
				// processed bundles
				lowestPendingMetadataPerPartition,
					highestNonPendingMetadataPerPartition := c.filterMetadataPerPartition(bundlesMetadata)
				// update pending metadata (the lowest offsets)
				c.patchMetadataMap(pendingBundleMetadataToCommit, lowestPendingMetadataPerPartition, true)
				// update processed metadata (the highest offsets)
				c.patchMetadataMap(processedBundleMetadataToCommit, highestNonPendingMetadataPerPartition, false)
			}

			// patch the processed bundle-metadata map with that of the pending ones, so that if a partition
			// has both types, the pending bundle gains priority (overwrites).
			for partition, BundleMetadata := range pendingBundleMetadataToCommit {
				processedBundleMetadataToCommit[partition] = BundleMetadata
			}

			if err := c.commitPositionsFunc(processedBundleMetadataToCommit); err != nil {
				c.log.Error(err, "committer failed")
			}
		}
	}
}

func (c *Committer) filterMetadataPerPartition(metadataArray []transport.BundleMetadata) ([]*BundleMetadata,
	[]*BundleMetadata) {
	// assumes all are in the same topic, TODO: support multi-topic when needed.
	lowestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)
	highestBundleMetadataPartitionsMap := make(map[int32]*BundleMetadata)

	for _, transportMetadata := range metadataArray {
		metadata, ok := transportMetadata.(*BundleMetadata)
		if !ok {
			continue // shouldn't happen
		}

		if !metadata.processed {
			// this belongs to a pending bundle, update the lowest-metadata-map
			if lowestMetadata, found := lowestBundleMetadataPartitionsMap[metadata.partition]; !found ||
				(found && metadata.offset < lowestMetadata.offset) {
				// if no offset was mapped to this partition or if a lower offset is found then update
				lowestBundleMetadataPartitionsMap[metadata.partition] = metadata
			}
		} else {
			// this belongs to a processed bundle, update the highest-metadata-map
			if highestMetadata, found := highestBundleMetadataPartitionsMap[metadata.partition]; !found ||
				(found && metadata.offset > highestMetadata.offset) {
				// if no offset was mapped to this partition or if a higher offset is found then update
				highestBundleMetadataPartitionsMap[metadata.partition] = metadata
			}
		}
	}

	// turn maps to arrays
	filteredPending := c.metadataMapToArray(lowestBundleMetadataPartitionsMap, true)
	filteredProcessed := c.metadataMapToArray(highestBundleMetadataPartitionsMap, false)

	return filteredPending, filteredProcessed
}

func (c *Committer) patchMetadataMap(data map[int32]*BundleMetadata,
	patch []*BundleMetadata, patchByLowest bool) {
	predicate := func(offset1, offset2 kafka.Offset) bool {
		return offset1 > offset2
	}

	if patchByLowest {
		predicate = func(offset1, offset2 kafka.Offset) bool {
			return offset1 < offset2
		}
	}

	for _, patchEntry := range patch {
		// check if the load's partition was already mapped
		if dataEntry, partitionFound := data[patchEntry.partition]; !partitionFound || (partitionFound &&
			predicate(patchEntry.offset, dataEntry.offset)) {
			data[patchEntry.partition] = patchEntry
		}
	}
}

func (c *Committer) metadataMapToArray(data map[int32]*BundleMetadata,
	decrementOffset bool) []*BundleMetadata {
	metadataArray := make([]*BundleMetadata, 0, len(data))

	for _, metadata := range data {
		if decrementOffset {
			// insert metadata of preceding bundle, which is safe to commit
			metadataArray = append(metadataArray, &BundleMetadata{
				topic:     metadata.topic,
				partition: metadata.partition,
				offset:    metadata.offset - 1,
			})
		} else {
			// these should be committed as-is, since they are not pending
			metadataArray = append(metadataArray, metadata)
		}
	}

	return metadataArray
}
