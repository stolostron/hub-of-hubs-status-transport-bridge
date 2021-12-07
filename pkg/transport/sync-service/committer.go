package syncservice

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const envVarCommitterInterval = "COMMITTER_INTERVAL"

// CommitObjectMetadataFunc is the function the sync-service transport provides for committing objects.
type CommitObjectMetadataFunc func(bundleMetadataMap map[string]*BundleMetadata) error

// NewCommitter returns a new instance of Committer.
func NewCommitter(log logr.Logger, commitObjectsMetadataFunc CommitObjectMetadataFunc) (*Committer, error) {
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
		log:                       log,
		commitObjectsMetadataFunc: commitObjectsMetadataFunc,
		consumers:                 make(map[transport.Consumer]struct{}),
		interval:                  committerInterval,
		lock:                      sync.Mutex{},
	}, nil
}

// Committer is responsible for committing offsets to transport.
type Committer struct {
	log                       logr.Logger
	commitObjectsMetadataFunc CommitObjectMetadataFunc
	consumers                 map[transport.Consumer]struct{}
	interval                  time.Duration
	lock                      sync.Mutex
}

// Start starts the Committer instance.
func (c *Committer) Start(ctx context.Context) {
	go c.commitMetadata(ctx)
}

// AddTransportConsumer adds a transport-Consumer that the committer will refer to when looking for offsets to commit.
func (c *Committer) AddTransportConsumer(user transport.Consumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, found := c.consumers[user]; !found {
		c.consumers[user] = struct{}{}
	}
}

func (c *Committer) commitMetadata(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C: // wait for next time interval
			processedBundleMetadataToCommit := make(map[string]*BundleMetadata)

			for consumer := range c.consumers {
				bundlesMetadata := consumer.GetBundlesMetadata()

				// sync service objects should be committed only if processed
				for _, bundleMetadata := range bundlesMetadata {
					metadata, ok := bundleMetadata.(*BundleMetadata)
					if !ok {
						continue // shouldn't happen
					}

					if metadata.processed {
						key := fmt.Sprintf("%s.%s", metadata.objectMetadata.ObjectID,
							metadata.objectMetadata.ObjectType)
						processedBundleMetadataToCommit[key] = metadata
					}
				}
			}

			if err := c.commitObjectsMetadataFunc(processedBundleMetadataToCommit); err != nil {
				c.log.Error(err, "committer failed")
			}
		}
	}
}
