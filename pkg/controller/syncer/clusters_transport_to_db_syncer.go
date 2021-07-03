package syncer

import (
	"errors"
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
)

func NewClustersTransportToDBSyncer(db hohDb.StatusTransportBridgeDB, transport transport.Transport, dbTableName string,
	bundleRegistration *BundleRegistration, stopChan chan struct{}) *ClustersTransportToDBSyncer {
	syncer := &ClustersTransportToDBSyncer{
		db:                          db,
		dbTableName:                 dbTableName,
		bundleUpdatesChan:           make(chan bundle.Bundle),
		lastBundleGenerationHandled: 0,
		stopChan:                    stopChan,
	}
	registerToBundleUpdates(transport, bundleRegistration, syncer.bundleUpdatesChan)
	log.Println(fmt.Sprintf("initialized syncer for table status.%s", dbTableName))
	return syncer
}

type ClustersTransportToDBSyncer struct {
	db                          hohDb.StatusTransportBridgeDB
	dbTableName                 string
	bundleUpdatesChan           chan bundle.Bundle
	lastBundleGenerationHandled uint64
	stopChan                    chan struct{}
}

func (s *ClustersTransportToDBSyncer) StartSync() {
	go s.syncBundles()
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (s *ClustersTransportToDBSyncer) syncBundles() {
	for {
		select { // wait for incoming bundles to handle
		case <-s.stopChan:
			return
		case receivedBundle := <-s.bundleUpdatesChan:
			if err := handleBundle(receivedBundle, &s.lastBundleGenerationHandled, s.handleBundle); err != nil {
				log.Println(err)
				// TODO schedule a retry, use exponential backoff
			}
		}
	}
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory
func (s *ClustersTransportToDBSyncer) handleBundle(bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	clustersFromDB, err := s.db.GetManagedClustersByLeafHub(s.dbTableName, leafHubName)
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
			if err = s.db.InsertManagedCluster(s.dbTableName, clusterName, leafHubName, object,
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
		if err = s.db.UpdateManagedCluster(s.dbTableName, clusterName, leafHubName, object,
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
		if err = s.db.DeleteManagedCluster(s.dbTableName, obj.ClusterName, leafHubName); err != nil {
			log.Println(fmt.Sprintf("error removing object %s from table status.%s", obj.ClusterName,
				s.dbTableName)) // failed to delete cluster from DB //TODO retry
		}
	}
	log.Println(fmt.Sprintf("finished handling 'ManagedClusters' bundle from leaf hub '%s', generation %d",
		leafHubName, bundle.GetGeneration()))

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
