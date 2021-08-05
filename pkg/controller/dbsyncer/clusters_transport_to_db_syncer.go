package dbsyncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/helpers"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// AddClustersTransportToDBSyncer adds clusters transport to db syncer to the manager.
func AddClustersTransportToDBSyncer(mgr ctrl.Manager, log logr.Logger, db hohDb.StatusTransportBridgeDB,
	transport transport.Transport, dbTableName string, bundleRegistration *transport.BundleRegistration) error {
	syncer := &ClustersTransportToDBSyncer{
		log:                            log,
		db:                             db,
		dbTableName:                    dbTableName,
		bundleUpdatesChan:              make(chan bundle.Bundle),
		bundlesGenerationLogPerLeafHub: make(map[string]*clusterBundlesGenerationLog),
	}

	transport.Register(bundleRegistration, syncer.bundleUpdatesChan)

	log.Info("initialized managed clusters syncer")

	return mgr.Add(syncer)
}

func newClusterBundlesGenerationLog() *clusterBundlesGenerationLog {
	return &clusterBundlesGenerationLog{
		lastBundleGeneration: 0,
	}
}

type clusterBundlesGenerationLog struct {
	lastBundleGeneration uint64
}

// ClustersTransportToDBSyncer implements managed clusters transport to db sync
type ClustersTransportToDBSyncer struct {
	log                            logr.Logger
	db                             hohDb.StatusTransportBridgeDB
	dbTableName                    string
	bundleUpdatesChan              chan bundle.Bundle
	bundlesGenerationLogPerLeafHub map[string]*clusterBundlesGenerationLog
}

// Start function starts the syncer.
func (syncer *ClustersTransportToDBSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.syncBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel

		syncer.log.Info("stopped managed clusters transport to db syncer")
		cancelContext()

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
		receivedBundle := <-syncer.bundleUpdatesChan
		leafHubName := receivedBundle.GetLeafHubName()
		syncer.createBundleGenerationLogIfNotExist(leafHubName)
		go func() {
			if err := helpers.HandleBundle(ctx, receivedBundle, &syncer.bundlesGenerationLogPerLeafHub[leafHubName].
				lastBundleGeneration, syncer.handleBundle); err != nil {
				syncer.log.Error(err, "failed to handle bundle,rescheduling") // TODO retry,use exponential backoff
				time.Sleep(time.Second)
				syncer.bundleUpdatesChan <- receivedBundle
			}
		}()
	}
}

// on the first time a new leaf hub connect, it needs to create bundle generation log, to manage the generation of
// the bundles we get from that specific leaf hub
func (syncer *ClustersTransportToDBSyncer) createBundleGenerationLogIfNotExist(leafHubName string) {
	if _, found := syncer.bundlesGenerationLogPerLeafHub[leafHubName]; !found {
		syncer.bundlesGenerationLogPerLeafHub[leafHubName] = newClusterBundlesGenerationLog()
	}
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory
func (syncer *ClustersTransportToDBSyncer) handleBundle(ctx context.Context, bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName, "Generation",
		bundle.GetGeneration())

	clustersFromDB, err := syncer.db.GetManagedClustersByLeafHub(ctx, syncer.dbTableName, leafHubName)
	if err != nil {
		return err
	}

	// TODO future optimization suggestion - send bundle sorted by id, get objects from db also sorted by id.
	// TODO then the search if object exists or not in the db should be O(1) instead of the existing state.
	for _, object := range bundle.GetObjects() {
		cluster := object.(metav1.Object)
		clusterName := cluster.GetName()
		index, err := getClusterIndexByName(clustersFromDB, clusterName)
		if err != nil { // cluster not found in the db table
			if err = syncer.db.InsertManagedCluster(ctx, syncer.dbTableName, clusterName, leafHubName, object,
				cluster.GetResourceVersion()); err != nil {
				log.Println(err) // failed to insert cluster to DB // TODO retry
			}
			continue
		}

		// if we got here, the object exists both in db and in the received bundle.
		clusterFromDB := clustersFromDB[index]
		clustersFromDB = append(clustersFromDB[:index], clustersFromDB[index+1:]...) // remove from objectsFromDB
		if cluster.GetResourceVersion() <= clusterFromDB.ResourceVersion {
			continue // sync object to db only if what we got is a newer version of the resource
		}

		if err = syncer.db.UpdateManagedCluster(ctx, syncer.dbTableName, clusterName, leafHubName, object,
			cluster.GetResourceVersion()); err != nil {
			log.Println(err) // failed to update cluster in DB //TODO retry
			continue
		}
	}
	// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources)
	for _, obj := range clustersFromDB {
		if obj == nil {
			continue
		}

		if err = syncer.db.DeleteManagedCluster(ctx, syncer.dbTableName, obj.ClusterName, leafHubName); err != nil {
			syncer.log.Error(err, "error removing object", "ObjectName", obj.ClusterName, "table",
				fmt.Sprintf("status.%s", syncer.dbTableName)) // TODO retry
		}
	}

	syncer.log.Info("finished handling 'ManagedClusters' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

func getClusterIndexByName(objects []*hohDb.ClusterKeyAndVersion, clusterName string) (int, error) {
	for i, object := range objects {
		if object.ClusterName == clusterName {
			return i, nil
		}
	}
	return -1, errors.New("object not found")
}
