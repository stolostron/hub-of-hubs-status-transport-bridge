package controller

import (
	"errors"
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"k8s.io/apimachinery/pkg/types"
	"log"
)

type genericTransportToDBSyncer struct {
	db                 hohDb.StatusTransportBridgeDB
	transport          transport.Transport
	dbTableName        string
	transportBundleKey string
	createBundleFunc   bundle.CreateBundleFunction
	bundleUpdatesChan  chan bundle.Bundle
	stopChan           chan struct{}
}

func (s *genericTransportToDBSyncer) Start() {
	s.init()
	go s.syncBundles()
}

func (s *genericTransportToDBSyncer) init() {
	s.bundleUpdatesChan = make(chan bundle.Bundle)
	s.transport.Register(s.transportBundleKey, s.bundleUpdatesChan, s.createBundleFunc)
	log.Println(fmt.Sprintf("initialzed syncer for table status.%s", s.dbTableName))
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using timestamp comparison and if the
// object was changed, update the db with the current object.
func (s *genericTransportToDBSyncer) syncBundles() {
	log.Println("start syncing...")
	for {
		select { // wait for incoming bundles to handle
		case <-s.stopChan:
			return
		case receivedBundle := <-s.bundleUpdatesChan:
			leafHubId := receivedBundle.GetLeafHubId()
			objectsFromDB, err := s.db.GetObjectsByLeafHub(s.dbTableName, leafHubId)
			if err != nil {
				log.Println(err) // TODO retry, not to lose updates. retry is relevant only if no other bundle received
				continue
			}
			for _, object := range receivedBundle.GetObjects() {
				objId := object.GetObjectId()
				index, err := getObjectIndexById(objectsFromDB, objId)
				if err != nil { // object not found in the db table
					if err = s.db.InsertManagedCluster(s.dbTableName, string(objId), leafHubId, object.GetObject(),
						object.GetLeafHubLastUpdateTimestamp()); err != nil {
						log.Println(err) // failed to insert object to DB // TODO retry
					}
					continue
				}
				// if we got here, the object exists both in db and in the received object.
				objectFromDB := objectsFromDB[index]
				objectsFromDB = append(objectsFromDB[:index], objectsFromDB[index+1:]...) // remove from objectsFromDB
				if !object.GetLeafHubLastUpdateTimestamp().After(objectFromDB.LastUpdateTimestamp) {
					continue // sync object to db only if something has changed, object hasn't changed...
				}
				if err = s.db.UpdateManagedCluster(s.dbTableName, string(objId), leafHubId, object.GetObject(),
					object.GetLeafHubLastUpdateTimestamp()); err != nil {
					log.Println(err) // TODO retry
				}
			}
		}
	}
}

func getObjectIndexById(objects []*hohDb.ObjectIdAndTimestamp, objId types.UID) (int, error) {
	for i, object := range objects {
		if object.ObjectId == objId {
			return i, nil
		}
	}
	return -1, errors.New("object id not found")
}
