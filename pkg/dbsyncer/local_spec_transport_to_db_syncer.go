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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NewLocalSpecDBSyncer creates a new instance of LocalSpecDBSyncer.
func NewLocalSpecDBSyncer(log logr.Logger, config *configv1.Config) DBSyncer {
	dbSyncer := &LocalSpecDBSyncer{
		log:                                     log,
		config:                                  config,
		createLocalPolicySpecBundleFunc:         bundle.NewLocalPolicySpecBundle,
		createLocalPlacementRulesSpecBundleFunc: bundle.NewLocalPlacementRulesBundle,
	}

	log.Info("initialized local spec db syncer")

	return dbSyncer
}

// LocalSpecDBSyncer implements local objects spec db sync business logic.
type LocalSpecDBSyncer struct {
	log                                     logr.Logger
	config                                  *configv1.Config
	createLocalPolicySpecBundleFunc         bundle.CreateBundleFunction
	createLocalPlacementRulesSpecBundleFunc bundle.CreateBundleFunction
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *LocalSpecDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	predicate := func() bool {
		return syncer.config.Spec.EnableLocalPolicies
	}

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPolicySpecMsgKey,
		CreateBundleFunc: syncer.createLocalPolicySpecBundleFunc,
		Predicate:        predicate,
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPlacementRulesMsgKey,
		CreateBundleFunc: syncer.createLocalPlacementRulesSpecBundleFunc,
		Predicate:        predicate,
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
	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPolicySpecPriority,
		helpers.GetBundleType(syncer.createLocalPolicySpecBundleFunc()),
		syncer.handleLocalObjectsBundleWrapper(db.LocalPolicySpecTableName)))

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPlacementRulesSpecPriority,
		helpers.GetBundleType(syncer.createLocalPlacementRulesSpecBundleFunc()),
		syncer.handleLocalObjectsBundleWrapper(db.LocalPlacementRulesTableName)))
}

func (syncer *LocalSpecDBSyncer) handleLocalObjectsBundleWrapper(tableName string) func(ctx context.Context,
	bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
	return func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
		return syncer.handleLocalObjectsBundle(ctx, bundle, db.LocalSpecSchema, tableName, dbClient)
	}
}

// handleLocalObjectsBundle generic function to handle bundles of local objects.
// if the row doesn't exist then add it.
// if the row exists then update it.
// if the row isn't in the bundle then delete it.
func (syncer *LocalSpecDBSyncer) handleLocalObjectsBundle(ctx context.Context, bundle bundle.Bundle, schema string,
	tableName string, dbClient db.LocalPoliciesStatusDB) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	idToVersionMapFromDB, err := dbClient.GetLocalDistinctIDAndVersion(ctx, schema, tableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s.%s' IDs from db - %w", schema, tableName, err)
	}

	batchBuilder := dbClient.NewGenericLocalBatchBuilder(schema, tableName, leafHubName)

	for _, object := range bundle.GetObjects() {
		specificObj, ok := object.(metav1.Object)
		if !ok {
			continue
		}

		uid := string(specificObj.GetUID())
		resourceVersionFromDB, objInDB := idToVersionMapFromDB[uid]

		if !objInDB { // object not found in the db table
			batchBuilder.Insert(object)
			continue
		}

		delete(idToVersionMapFromDB, uid)

		if specificObj.GetResourceVersion() == resourceVersionFromDB {
			continue // update object in db only if what we got is a different (newer) version of the resource.
		}

		batchBuilder.Update(object)
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
