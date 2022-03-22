package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewSubscriptionsDBSyncer creates a new instance of SubscriptionsDBSyncer.
func NewSubscriptionsDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &SubscriptionsDBSyncer{
		log:                          log,
		createSubscriptionBundleFunc: bundle.NewSubscriptionsStatusBundle,
	}

	log.Info("initialized subscriptions db syncer")

	return dbSyncer
}

// SubscriptionsDBSyncer implements subscriptions db sync business logic.
type SubscriptionsDBSyncer struct {
	log                          logr.Logger
	createSubscriptionBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *SubscriptionsDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.SubscriptionStatusMsgKey,
		CreateBundleFunc: syncer.createSubscriptionBundleFunc,
		Predicate:        func() bool { return true }, // always get subscription status
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the conflation manager.
// handler function need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *SubscriptionsDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.SubscriptionsStatusPriority,
		status.CompleteStateMode,
		helpers.GetBundleType(syncer.createSubscriptionBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleSubscriptionsStatusBundle(ctx, bundle, dbClient, db.StatusSchema, db.SubscriptionTableName)
		},
	))
}

func (syncer *SubscriptionsDBSyncer) handleSubscriptionsStatusBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.SubscriptionsStatusDB, schema string, tableName string, ) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	idToVersionMapFromDB, err := dbClient.GetDistinctIDAndVersion(ctx, schema, tableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s.%s' IDs from db - %w", schema, tableName, err)
	}

	batchBuilder := dbClient.NewGenericBatchBuilder(schema, tableName, leafHubName)

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
