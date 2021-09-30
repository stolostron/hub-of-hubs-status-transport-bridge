package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"k8s.io/apimachinery/pkg/types"
)

type specDBObj interface {
	GetName() string
	GetUID() types.UID
}

// NewLocalSpecDBSyncer creates a new instance of PoliciesDBSyncer.
func NewLocalSpecDBSyncer(log logr.Logger, config *configv1.Config) DBSyncer {
	dbSyncer := &LocalSpecDBSyncer{
		log:                                log,
		config:                             config,
		createLocalPolicySpecBundleFunc:    func() bundle.Bundle { return bundle.NewLocalSpecBundle() },
		createLocalPlacementSpecBundleFunc: func() bundle.Bundle { return bundle.NewLocalPlacementRuleBundle() },
	}

	log.Info("initialized local spec db syncer")

	return dbSyncer
}

// LocalSpecDBSyncer implements policies db sync business logic.
type LocalSpecDBSyncer struct {
	log                                logr.Logger
	config                             *configv1.Config
	createLocalPolicySpecBundleFunc    func() bundle.Bundle
	createLocalPlacementSpecBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *LocalSpecDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPlacementRulesMsgKey,
		CreateBundleFunc: syncer.createLocalPlacementSpecBundleFunc,
		Predicate:        func() bool { return true }, // temporary
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalSpecPerPolicyMsgKey,
		CreateBundleFunc: syncer.createLocalPolicySpecBundleFunc,
		Predicate:        func() bool { return true }, // temporary
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the dispatcher.
// handler functions need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *LocalSpecDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	localPolicySpecBundleType := helpers.GetBundleType(syncer.createLocalPolicySpecBundleFunc())
	localPlacementRuleSpecBundleType := helpers.GetBundleType(syncer.createLocalPlacementSpecBundleFunc())
	// when getting an error that cluster does not exist, turn implicit dependency on MC bundle to explicit dependency
	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPolicySpecPriority,
		localPolicySpecBundleType,
		syncer.handleLocalSpecBundle))

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPlacementRuleSpecPriority,
		localPlacementRuleSpecBundleType,
		syncer.handleLocalPlacementRule))
}

func (syncer *LocalSpecDBSyncer) handleLocalSpecBundle(ctx context.Context, b bundle.Bundle,
	db db.StatusTransportBridgeDB) error {
	leafHubName := b.GetLeafHubName()
	bundleGen := b.GetGeneration()

	syncer.log.Info("start handling 'LocalPolicySpec' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGen)

	if err := syncer.handleLocalSpecBundleHelper(ctx, b, LocalPolicySpecTableName, db); err != nil {
		return err
	}

	syncer.log.Info("finished handling 'LocalPolicySpec' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGen)

	return nil
}

func (syncer *LocalSpecDBSyncer) handleLocalPlacementRule(ctx context.Context, b bundle.Bundle,
	db db.StatusTransportBridgeDB) error {
	leafHubName := b.GetLeafHubName()
	bundleGen := b.GetGeneration()

	syncer.log.Info("start handling 'LocalPlacementRule' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGen)

	if err := syncer.handleLocalSpecBundleHelper(ctx, b, LocalPlacementRuleTableName, db); err != nil {
		return err
	}

	syncer.log.Info("finished handling 'LocalPlacementRule' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGen)

	return nil
}

// if the row doesn't exist then add it.
// if the row exists then update it.
// if the row isn't in the bundle then delete it.
// saves the json file in the DB.
func (syncer *LocalSpecDBSyncer) handleLocalSpecBundleHelper(ctx context.Context, b bundle.Bundle,
	tableName string, db db.StatusTransportBridgeDB) error {
	leafHubName := b.GetLeafHubName()

	objectIDsFromDB, err := db.GetDistinctIDsFromLH(ctx, tableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' IDs from db - %w", leafHubName, err)
	}

	for _, object := range b.GetObjects() {
		specificObj, ok := object.(specDBObj)
		if !ok {
			continue
		}

		specificObjInd, err := helpers.GetObjectIndex(objectIDsFromDB, string(specificObj.GetUID()))
		if err != nil { // usefulObj not found, new specificObj id
			if err = db.InsertIntoSpecSchema(ctx, string(specificObj.GetUID()),
				tableName, leafHubName, object); err != nil {
				return fmt.Errorf("failed inserting '%s' from leaf hub '%s' - %w",
					specificObj.GetName(), leafHubName, err)
			}
			// we can continue since its not in objectIDsFromDB anyway
			continue
		}
		// since this already exists in the db and in the bundle we need to update it
		err = db.UpdateSingleSpecRow(ctx, string(specificObj.GetUID()), leafHubName,
			tableName, object)
		if err != nil {
			return fmt.Errorf(`failed updating spec '%s' in leaf hub '%s' 
					in db - %w`, string(specificObj.GetUID()), leafHubName, err)
		}

		// we dont want to delete it later
		objectIDsFromDB = append(objectIDsFromDB[:specificObjInd], objectIDsFromDB[specificObjInd+1:]...)
	}

	err = syncer.deleteLocalSpecRows(ctx, leafHubName, tableName, objectIDsFromDB, db)
	if err != nil {
		return err
	}

	return nil
}

func (syncer *LocalSpecDBSyncer) deleteLocalSpecRows(ctx context.Context, leafHubName string,
	tableName string, policyIDToDelete []string, db db.StatusTransportBridgeDB) error {
	for _, id := range policyIDToDelete {
		err := db.DeleteSingleSpecRow(ctx, leafHubName, tableName, id)
		if err != nil {
			return fmt.Errorf("failed deleting policy with id %s from leaf hub %s from table %s - %w", id,
				leafHubName, tableName, err)
		}
	}

	return nil
}
