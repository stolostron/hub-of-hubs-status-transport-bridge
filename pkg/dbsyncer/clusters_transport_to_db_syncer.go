package dbsyncer

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	managedclustersv1 "github.com/open-cluster-management/api/cluster/v1"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

var errObjectNotManagedCluster = errors.New("failed to parse object in bundle to a managed cluster")

// NewManagedClustersDBSyncer creates a new instance of ManagedClustersDBSyncer.
func NewManagedClustersDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &ManagedClustersDBSyncer{
		log:              log,
		createBundleFunc: func() bundle.Bundle { return bundle.NewManagedClustersStatusBundle() },
	}

	log.Info("initialized managed clusters db syncer")

	return dbSyncer
}

// ManagedClustersDBSyncer implements managed clusters db sync business logic.
type ManagedClustersDBSyncer struct {
	log              logr.Logger
	createBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *ManagedClustersDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.ManagedClustersMsgKey,
		CreateBundleFunc: syncer.createBundleFunc,
		Predicate:        func() bool { return true }, // always get managed clusters bundles
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the conflation manager.
// handler function need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *ManagedClustersDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	conflationManager.Register(&conflator.ConflationRegistration{
		Priority:        conflator.ManagedClustersPriority,
		BundleType:      helpers.GetBundleType(syncer.createBundleFunc()),
		HandlerFunction: syncer.handleManagedClustersBundle,
		SyncMode:        conflator.CompleteStateSyncMode,
	})
}

func (syncer *ManagedClustersDBSyncer) handleManagedClustersBundle(ctx context.Context, bundle bundle.Bundle,
	dbConn db.StatusTransportBridgeDB) error {
	leafHubName := bundle.GetLeafHubName()
	version := bundle.GetVersion()
	syncer.log.Info("start handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName, "Version",
		helpers.FormatBundleVersion(version))

	clustersFromDB, err := dbConn.GetManagedClustersByLeafHub(ctx, managedClustersTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub managed clusters from db - %w", err)
	}

	for _, object := range bundle.GetObjects() {
		cluster, ok := object.(*managedclustersv1.ManagedCluster)
		if !ok {
			syncer.log.Error(errObjectNotManagedCluster, "skipping object")
			continue // do not handle objects other than ManagedCluster
		}

		clusterName := cluster.GetName()
		resourceVersionFromDB, clusterExists := clustersFromDB[clusterName]

		if !clusterExists { // cluster not found in the db table
			if err = dbConn.InsertManagedCluster(ctx, managedClustersTableName, leafHubName, clusterName, object,
				cluster.GetResourceVersion()); err != nil {
				return fmt.Errorf("failed to insert cluster '%s' from leaf hub '%s' to the DB - %w", clusterName,
					leafHubName, err)
			}

			continue
		}
		// if we got here, the managed cluster exists both in db and in the received bundle.
		delete(clustersFromDB, clusterName)

		if cluster.GetResourceVersion() == resourceVersionFromDB {
			continue // sync cluster to db only if what we got is a different version of the resource
		}

		if err = dbConn.UpdateManagedCluster(ctx, managedClustersTableName, leafHubName, clusterName, object,
			cluster.GetResourceVersion()); err != nil {
			return fmt.Errorf("failed to update cluster '%s' from leaf hub '%s' in the DB - %w", clusterName,
				leafHubName, err)
		}
	}
	// delete clusters that in the db but were not sent in the bundle (leaf hub sends only living resources).
	if err = syncer.deleteClustersFromDB(ctx, dbConn, leafHubName, clustersFromDB); err != nil {
		return fmt.Errorf("failed deleting clusters from db - %w", err)
	}

	syncer.log.Info("finished handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName,
		"Version", helpers.FormatBundleVersion(version))

	return nil
}

func (syncer *ManagedClustersDBSyncer) deleteClustersFromDB(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	leafHubName string, clustersFromDB map[string]string) error {
	for clusterName := range clustersFromDB {
		if err := dbConn.DeleteManagedCluster(ctx, managedClustersTableName, leafHubName, clusterName); err != nil {
			return fmt.Errorf("failed to delete cluster '%s' from leaf hub '%s' from the DB - %w",
				clusterName, leafHubName, err)
		}
	}

	return nil
}
