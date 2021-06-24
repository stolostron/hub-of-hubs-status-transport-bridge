package controller

import (
	"errors"
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
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
	log.Println(fmt.Sprintf("initialized syncer for table status.%s", s.dbTableName))
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using timestamp comparison and if the
// object was changed, update the db with the current object.
func (s *genericTransportToDBSyncer) syncBundles() {
	for {
		select { // wait for incoming bundles to handle
		case <-s.stopChan:
			return
		case receivedBundle := <-s.bundleUpdatesChan:
			leafHubName := receivedBundle.GetLeafHubName()
			log.Println(fmt.Sprintf("received bundle '%s' from leaf hub %s", s.transportBundleKey, leafHubName))
			objectsFromDB, err := s.db.GetObjectsByLeafHub(s.dbTableName, leafHubName)
			if err != nil {
				log.Println(err)
				// TODO retry on error, not to lose updates. retry is relevant only if no other bundle received
				// TODO should use exponential backoff.
				continue
			}
			// TODO future optimization suggestion - send bundle sorted by id, get objects from db also sorted by id.
			// TODO then the search if object exists or not in the db should be O(1) instead of the existing state.
			for _, object := range receivedBundle.GetObjects() {
				objName := object.GetName()
				index, err := getObjectIndexByName(objectsFromDB, objName)
				if err != nil { // object not found in the db table
					if err = s.db.InsertManagedCluster(s.dbTableName, objName, leafHubName, object,
						object.GetResourceVersion()); err != nil {
						log.Println(err) // failed to insert object to DB // TODO retry
					}
					continue
				}
				// if we got here, the object exists both in db and in the received bundle.
				objectFromDB := objectsFromDB[index]
				objectsFromDB = append(objectsFromDB[:index], objectsFromDB[index+1:]...) // remove from objectsFromDB
				if object.GetResourceVersion() <= objectFromDB.ResourceVersion {
					continue // sync object to db only if what we got is a newer version of the resource
				}
				if err = s.db.UpdateManagedCluster(s.dbTableName, objName, leafHubName, object,
					object.GetResourceVersion()); err != nil {
					log.Println(err) // TODO retry
					continue
				}
			}
			// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources)
			for _, obj := range objectsFromDB {
				if obj == nil {
					continue
				}
				if err = s.db.DeleteManagedCluster(s.dbTableName, obj.ObjectName, leafHubName); err != nil {
					log.Println(fmt.Sprintf("error removing object %s from table status.%s", obj.ObjectName,
						s.dbTableName))
				}
			}
		}
	}
}

func getObjectIndexByName(objects []*hohDb.ObjectNameAndVersion, objName string) (int, error) {
	for i, object := range objects {
		if object.ObjectName == objName {
			return i, nil
		}
	}
	return -1, errors.New("object not found")
}
