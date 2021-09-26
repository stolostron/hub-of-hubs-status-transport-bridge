package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	subv1 "github.com/open-cluster-management/multicloud-operators-subscription/pkg/apis/apps/v1"
)

// NewApplicationDBSyncer creates a new instance of ManagedClustersDBSyncer.
func NewApplicationDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &ApplicationDBSyncer{
		log:              log,
		createBundleFunc: func() bundle.Bundle { return bundle.NewSubscriptionBundle() },
	}

	log.Info("initialized application db syncer")

	return dbSyncer
}

// ApplicationDBSyncer implements managed clusters db sync business logic.
type ApplicationDBSyncer struct {
	log              logr.Logger
	createBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *ApplicationDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.SubscriptionStatusMsgKey,
		CreateBundleFunc: syncer.createBundleFunc,
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
func (syncer *ApplicationDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	conflationManager.Register(&conflator.ConflationRegistration{
		Priority:        conflator.SubscriptionStatusPriority,
		BundleType:      helpers.GetBundleType(syncer.createBundleFunc()),
		HandlerFunction: syncer.handleSubscriptionBundle,
	})
}

func (syncer *ApplicationDBSyncer) handleSubscriptionBundle(ctx context.Context, bundle bundle.Bundle,
	dbConn db.StatusTransportBridgeDB) error {
	leafHubName := bundle.GetLeafHubName()

	objectIDsFromDB, err := dbConn.GetDistinctIDsFromLH(ctx, subscriptionTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' IDs from db - %w", leafHubName, err)
	}

	syncer.log.Info(fmt.Sprintf("amount is %d and leaf hub name is %s", len(objectIDsFromDB), leafHubName))
	syncer.log.Info(fmt.Sprintf("%#v", objectIDsFromDB[0]))

	for _, object := range bundle.GetObjects() {
		subscription, ok := object.(*subv1.Subscription)
		if !ok {
			continue
		}

		subscriptionInd, err := helpers.GetObjectIndex(objectIDsFromDB, string(subscription.GetUID()))
		if err != nil { // subscription not found, new subscription id
			syncer.log.Info("ASDASDASD")

			if err = dbConn.InsertNewSubscriptionRow(ctx, string(subscription.GetUID()), leafHubName,
				subscription, string(subscription.Status.Phase), subscription.ResourceVersion,
				subscriptionTableName); err != nil {
				return fmt.Errorf("failed inserting '%s' from leaf hub '%s' - %w",
					subscription.GetName(), leafHubName, err)
			}
			// we can continue since its not in objectIDsFromDB anyway
			continue
		}
		// since this already exists in the db and in the bundle we need to update it
		err = dbConn.UpdateSingleSpecRow(ctx, string(subscription.GetUID()), leafHubName,
			subscriptionTableName, object)
		if err != nil {
			return fmt.Errorf(`failed updating status '%s' in leaf hub '%s' 
					in db - %w`, string(subscription.GetUID()), leafHubName, err)
		}

		// we dont want to delete it later
		objectIDsFromDB = append(objectIDsFromDB[:subscriptionInd], objectIDsFromDB[subscriptionInd+1:]...)
	}

	syncer.log.Info("END")

	err = syncer.deleteSubscriptionRows(ctx, leafHubName, subscriptionTableName, objectIDsFromDB, dbConn)
	if err != nil {
		return err
	}

	return nil
}

func (syncer *ApplicationDBSyncer) deleteSubscriptionRows(ctx context.Context, leafHubName string,
	tableName string, policyIDToDelete []string, dbConn db.StatusTransportBridgeDB) error {
	for _, id := range policyIDToDelete {
		err := dbConn.DeleteSingleSubscriptionRow(ctx, leafHubName, tableName, id)
		if err != nil {
			return fmt.Errorf("failed deleting %s from leaf hub %s from table %s - %w", id,
				leafHubName, tableName, err)
		}
	}

	return nil
}
