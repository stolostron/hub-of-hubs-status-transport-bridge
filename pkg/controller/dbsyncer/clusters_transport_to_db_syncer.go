package dbsyncer

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var errObjectNotFound = errors.New("object not found")

// AddClustersTransportToDBSyncer adds clusters transport to db syncer to the manager.
func AddClustersTransportToDBSyncer(mgr ctrl.Manager, log logr.Logger, dbWorkerPool *workerpool.DBWorkerPool,
	transport transport.Transport, dbTableName string,
	bundleRegistration *transport.BundleRegistration) error {
	syncer := &ClustersTransportToDBSyncer{
		log:                          log,
		dbWorkerPool:                 dbWorkerPool,
		dbTableName:                  dbTableName,
		bundleUpdatesChan:            make(chan bundle.Bundle),
		bundlesAttempedGenerationLog: newBundlesGenerationLog(),
		leafHubsLocks:                newLeafHubsLocks(),
	}

	transport.Register(bundleRegistration, syncer.bundleUpdatesChan)

	log.Info("initialized managed clusters syncer")

	if err := mgr.Add(syncer); err != nil {
		return fmt.Errorf("failed to add transport to db syncer to manager - %w", err)
	}

	return nil
}

// ClustersTransportToDBSyncer implements managed clusters transport to db sync.
type ClustersTransportToDBSyncer struct {
	log                          logr.Logger
	dbWorkerPool                 *workerpool.DBWorkerPool
	dbTableName                  string
	bundleUpdatesChan            chan bundle.Bundle
	bundlesAttempedGenerationLog *bundlesGenerationLog
	leafHubsLocks                *leafHubsLocks
}

// Start function starts the syncer.
func (syncer *ClustersTransportToDBSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.syncBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel
		cancelContext()
		syncer.log.Info("stopped managed clusters transport to db syncer")

		return nil
	}
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *ClustersTransportToDBSyncer) syncBundles(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case receivedBundle := <-syncer.bundleUpdatesChan:
			syncer.leafHubsLocks.lockLeafHub(receivedBundle.GetLeafHubName())
			handleBundle(receivedBundle, syncer.bundlesAttempedGenerationLog, syncer.handleBundle, syncer.dbWorkerPool,
				func(bundle bundle.Bundle) { syncer.leafHubsLocks.unlockLeafHub(bundle.GetLeafHubName()) }, // success
				func(err error) { // error
					syncer.leafHubsLocks.unlockLeafHub(receivedBundle.GetLeafHubName())
					syncer.log.Error(err, "failed to handle bundle")
					handleRetry(receivedBundle, syncer.bundleUpdatesChan)
				})
		}
	}
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory.
func (syncer *ClustersTransportToDBSyncer) handleBundle(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName, "Generation",
		bundle.GetGeneration())

	clustersFromDB, err := dbConn.GetManagedClustersByLeafHub(ctx, syncer.dbTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub managed clusters from db - %w", err)
	}

	for _, object := range bundle.GetObjects() {
		cluster, ok := object.(metav1.Object)
		if !ok {
			continue // do not handle objects other than metav1.Object
		}

		clusterName := cluster.GetName()

		index, err := getClusterIndexByName(clustersFromDB, clusterName)
		if err != nil { // cluster not found in the db table
			if err = dbConn.InsertManagedCluster(ctx, syncer.dbTableName, clusterName, leafHubName, object,
				cluster.GetResourceVersion()); err != nil {
				return fmt.Errorf("failed to insert cluster '%s' from leaf hub '%s' to the DB - %w", clusterName,
					leafHubName, err)
			}

			continue
		}
		// if we got here, the object exists both in db and in the received bundle.
		clusterFromDB := clustersFromDB[index]
		clustersFromDB = append(clustersFromDB[:index], clustersFromDB[index+1:]...) // remove from objectsFromDB

		if cluster.GetResourceVersion() <= clusterFromDB.ResourceVersion {
			continue // sync object to db only if what we got is a newer version of the resource
		}

		if err = dbConn.UpdateManagedCluster(ctx, syncer.dbTableName, clusterName, leafHubName, object,
			cluster.GetResourceVersion()); err != nil {
			return fmt.Errorf("failed to update cluster '%s' from leaf hub '%s' in the DB - %w", clusterName,
				leafHubName, err)
		}
	}
	// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources).
	if err = syncer.deleteClustersFromDB(ctx, dbConn, leafHubName, clustersFromDB); err != nil {
		return fmt.Errorf("failed deleting clusters from db - %w", err)
	}

	syncer.log.Info("finished handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

func (syncer *ClustersTransportToDBSyncer) deleteClustersFromDB(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	leafHubName string, clustersFromDB []*db.ClusterKeyAndVersion,
) error {
	for _, obj := range clustersFromDB {
		if obj == nil {
			continue
		}

		if err := dbConn.DeleteManagedCluster(ctx, syncer.dbTableName, obj.ClusterName, leafHubName); err != nil {
			return fmt.Errorf("failed to delete cluster '%s' from leaf hub '%s' from the DB - %w",
				obj.ClusterName, leafHubName, err)
		}
	}

	return nil
}

func getClusterIndexByName(objects []*db.ClusterKeyAndVersion, clusterName string) (int, error) {
	for i, object := range objects {
		if object.ClusterName == clusterName {
			return i, nil
		}
	}

	return -1, fmt.Errorf("%w - %s", errObjectNotFound, clusterName)
}
