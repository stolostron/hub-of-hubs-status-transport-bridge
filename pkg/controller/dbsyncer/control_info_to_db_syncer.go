package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddControlInfoTransportToDBSyncer adds control info transport to db syncer to the manager.
func AddControlInfoTransportToDBSyncer(mgr ctrl.Manager, log logr.Logger, dbWorkerPool *workerpool.DBWorkerPool,
	transport transport.Transport, dbTableName string,
	bundleRegistration *transport.BundleRegistration) error {
	syncer := &ControlInfoTransportToDBSyncer{
		log:               log,
		dbWorkerPool:      dbWorkerPool,
		dbTableName:       dbTableName,
		bundleUpdatesChan: make(chan bundle.Bundle),
	}

	transport.Register(bundleRegistration, syncer.bundleUpdatesChan)

	log.Info("initialized control info syncer")

	if err := mgr.Add(syncer); err != nil {
		return fmt.Errorf("failed to add transport to db syncer to manager - %w", err)
	}

	return nil
}

// ControlInfoTransportToDBSyncer implements control info transport to db sync.
type ControlInfoTransportToDBSyncer struct {
	log               logr.Logger
	dbWorkerPool      *workerpool.DBWorkerPool
	dbTableName       string
	bundleUpdatesChan chan bundle.Bundle
}

// Start function starts the syncer.
func (syncer *ControlInfoTransportToDBSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.syncBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel
		cancelContext()
		syncer.log.Info("stopped control info transport to db syncer")

		return nil
	}
}

func (syncer *ControlInfoTransportToDBSyncer) syncBundles(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case receivedBundle := <-syncer.bundleUpdatesChan:
			syncer.dbWorkerPool.QueueDBJob(&workerpool.DBJob{
				HandlerFunc: func(ctx context.Context, dbConn db.StatusTransportBridgeDB) {
					if err := dbConn.UpdateHeartbeat(ctx, syncer.dbTableName, receivedBundle.GetLeafHubName()); err != nil {
						syncer.log.Error(err, "failed to handle bundle")
					}
				},
			})
		}
	}
}
