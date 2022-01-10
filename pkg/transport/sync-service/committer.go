package syncservice

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/open-horizon/edge-sync-service-client/client"
)

const envVarCommitterInterval = "COMMITTER_INTERVAL"

// newCommitter returns a new instance of Committer.
func newCommitter(log logr.Logger, client *client.SyncServiceClient,
	getBundlesMetadataFunc transport.GetBundlesMetadataFunc) (*committer, error) {
	committerIntervalString, found := os.LookupEnv(envVarCommitterInterval)
	if !found {
		return nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarCommitterInterval)
	}

	committerInterval, err := time.ParseDuration(committerIntervalString)
	if err != nil {
		return nil, fmt.Errorf("the environment var %s is not valid duration - %w",
			committerIntervalString, err)
	}

	return &committer{
		log:                           log,
		client:                        client,
		getBundlesMetadataFunc:        getBundlesMetadataFunc,
		committedMetadataToVersionMap: make(map[string]string),
		interval:                      committerInterval,
	}, nil
}

// committer is responsible for committing offsets to transport.
type committer struct {
	log                           logr.Logger
	client                        *client.SyncServiceClient
	getBundlesMetadataFunc        transport.GetBundlesMetadataFunc
	committedMetadataToVersionMap map[string]string
	interval                      time.Duration
}

// start runs the Committer instance.
func (c *committer) start(ctx context.Context) {
	go c.commitMetadata(ctx)
}

func (c *committer) commitMetadata(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C: // wait for next time interval
			processedBundleMetadataToCommit := make(map[string]*bundleMetadata)

			metadataArray := c.getBundlesMetadataFunc()
			// sync service objects should be committed only if processed
			for _, transportMetadata := range metadataArray {
				metadata, ok := transportMetadata.(*bundleMetadata)
				if !ok {
					continue // shouldn't happen
				}

				if metadata.Processed() {
					key := fmt.Sprintf("%s.%s", metadata.objectMetadata.ObjectID,
						metadata.objectMetadata.ObjectType)
					processedBundleMetadataToCommit[key] = metadata
				}
			}

			if err := c.commitObjectsMetadata(processedBundleMetadataToCommit); err != nil {
				c.log.Error(err, "committer failed")
			}
		}
	}
}

func (c *committer) commitObjectsMetadata(bundleMetadataMap map[string]*bundleMetadata) error {
	for key, bundleMetadata := range bundleMetadataMap {
		if version, found := c.committedMetadataToVersionMap[key]; found {
			if version == bundleMetadata.objectMetadata.Version {
				continue // already committed
			}
		}

		if err := c.client.MarkObjectConsumed(bundleMetadata.objectMetadata); err != nil {
			return fmt.Errorf("failed to commit object - stopping bulk commit : %w", err)
		}

		c.committedMetadataToVersionMap[key] = bundleMetadata.objectMetadata.Version
	}

	return nil
}
