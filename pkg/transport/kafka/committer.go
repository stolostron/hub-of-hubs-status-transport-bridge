package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const envVarCommitterInterval = "COMMITTER_INTERVAL"

var errEnvVarNotFound = errors.New("not found environment variable")

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
func (c *Committer) Start(stopChannel <-chan struct{}) {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go c.commitOffsets(ctx)
	c.log.Info("started")

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel
		cancelContext()
		c.log.Info("stopped")
	}
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
			bundleMetadataToCommit := make(map[int32]*BundleMetadata)

			// fill map with the lowest offsets per partition via iterating over all registered consumers
			for Consumer := range c.consumers {
				// get metadata (pending, non-pending) from Consumer
				bundlesMetadata := Consumer.GetBundlesMetadata()
				// extract the lowest per partition in the pending bundles, the highest per partition in the
				// processed bundles
				lowestPendingMetadataPerPartition,
					highestNonPendingMetadataPerPartition := c.filterMetadataPerPartition(bundlesMetadata)
				// update bundleMetadataToCommit map with both, the same way. patch the map by lowest metadata
				// per partition, so if two bundles on the same partition are in each collection above, the lowest is
				// committed (pending bundle offset - 1 or non-pending bundle offset)
				// NOTICE: the non-pending must be patched first so that the pending ones can overwrite.
				c.patchCommitMetadataMap(bundleMetadataToCommit, highestNonPendingMetadataPerPartition, false)
				c.patchCommitMetadataMap(bundleMetadataToCommit, lowestPendingMetadataPerPartition, true)
			}

			if err := c.commitPositionsFunc(bundleMetadataToCommit); err != nil {
				c.log.Error(err, "committer failed", "bundles metadata sent", *bundleMetadataToCommit[0])
			}

			if len(bundleMetadataToCommit) > 0 {
				c.log.Info("attempted commit", "bundles metadata sent", *bundleMetadataToCommit[0])
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
			return nil, nil
		}

		if !metadata.Processed {
			// this belongs to a pending bundle, update the lowest-metadata-map
			if lowestMetadata, found := lowestBundleMetadataPartitionsMap[metadata.Partition]; !found ||
				(found && metadata.Offset < lowestMetadata.Offset) {
				// if no offset was mapped to this partition or if a lower offset is found then update
				lowestBundleMetadataPartitionsMap[metadata.Partition] = metadata
			}
		} else {
			// this belongs to a processed bundle, update the highest-metadata-map
			if highestMetadata, found := highestBundleMetadataPartitionsMap[metadata.Partition]; !found ||
				(found && metadata.Offset > highestMetadata.Offset) {
				// if no offset was mapped to this partition or if a higher offset is found then update
				highestBundleMetadataPartitionsMap[metadata.Partition] = metadata
			}
		}
	}

	// turn maps to arrays
	filteredPending := c.metadataMapToArray(lowestBundleMetadataPartitionsMap, true)
	filteredProcessed := c.metadataMapToArray(highestBundleMetadataPartitionsMap, false)

	return filteredPending, filteredProcessed
}

func (c *Committer) patchCommitMetadataMap(data map[int32]*BundleMetadata,
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
		if dataEntry, partitionFound := data[patchEntry.Partition]; !partitionFound || (partitionFound &&
			predicate(patchEntry.Offset, dataEntry.Offset)) {
			data[patchEntry.Partition] = patchEntry
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
				Topic:     metadata.Topic,
				Partition: metadata.Partition,
				Offset:    metadata.Offset - 1,
			})
		} else {
			// these should be committed as-is, since they are not pending
			metadataArray = append(metadataArray, metadata)
		}
	}

	return metadataArray
}
