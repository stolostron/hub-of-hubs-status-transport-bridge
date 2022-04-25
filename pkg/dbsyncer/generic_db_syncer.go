package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// genericDBSyncer implements generic status resource db sync business logic.
type genericDBSyncer struct {
	log             logr.Logger
	transportMsgKey string

	dbSchema    string
	dbTableName string

	createBundleFunc func() bundle.Bundle
	bundlePriority   conflator.ConflationPriority
	bundleSyncMode   status.BundleSyncMode
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *genericDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            syncer.transportMsgKey,
		CreateBundleFunc: syncer.createBundleFunc,
		Predicate:        func() bool { return true }, // always get generic status resources
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the conflation manager.
// handler function need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *genericDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	conflationManager.Register(conflator.NewConflationRegistration(
		syncer.bundlePriority,
		syncer.bundleSyncMode,
		helpers.GetBundleType(syncer.createBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleResourcesBundle(ctx, bundle, dbClient)
		},
	))
}

func (syncer *genericDBSyncer) handleResourcesBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.StatusTransportBridgeDB) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	idToVersionMapFromDB, err := dbClient.GetDistinctIDAndVersion(ctx, syncer.dbSchema, syncer.dbTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s.%s' IDs from db - %w", syncer.dbSchema, syncer.dbTableName, err)
	}

	batchBuilder := dbClient.NewGenericBatchBuilder(syncer.dbSchema, syncer.dbTableName, leafHubName)

	for _, object := range bundle.GetObjects() {
		specificObj, ok := object.(metav1.Object)
		if !ok {
			continue
		}

		uid := string(specificObj.GetUID())
		resourceVersionFromDB, objExistsInDB := idToVersionMapFromDB[uid]

		if !objExistsInDB { // object not found in the db table
			batchBuilder.Insert(uid, object)
			continue
		}

		delete(idToVersionMapFromDB, uid)

		if specificObj.GetResourceVersion() == resourceVersionFromDB {
			continue // update object in db only if what we got is a different (newer) version of the resource.
		}

		batchBuilder.Update(uid, object)
	}

	// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources).
	for uid := range idToVersionMapFromDB {
		batchBuilder.Delete(uid)
	}

	if err := dbClient.SendBatch(ctx, batchBuilder.Build()); err != nil {
		return fmt.Errorf("failed to perform batch - %w", err)
	}

	logBundleHandlingMessage(syncer.log, bundle, finishBundleHandlingMessage)

	return nil
}
