package dbsyncer

import (
	"context"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewLocalSpecDBSyncer creates a new instance of PoliciesDBSyncer.
func NewLocalSpecDBSyncer(log logr.Logger, config *configv1.Config) DBSyncer {
	dbSyncer := &LocalSpecDBSyncer{
		log:                                    log,
		config:                                 config,
		createLocalPolicySpecBundleFunc:        bundle.NewLocalPolicySpecBundle,
		createLocalPlacementRuleSpecBundleFunc: bundle.NewLocalPlacementRuleBundle,
	}

	log.Info("initialized local spec db syncer")

	return dbSyncer
}

// LocalSpecDBSyncer implements policies db sync business logic.
type LocalSpecDBSyncer struct {
	log                                    logr.Logger
	config                                 *configv1.Config
	createLocalPolicySpecBundleFunc        func() bundle.Bundle
	createLocalPlacementRuleSpecBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *LocalSpecDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	predicate := func() bool {
		return syncer.config.Spec.AggregationLevel == configv1.Full &&
			syncer.config.Spec.EnableLocalPolicies
	}

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPolicySpecMsgKey,
		CreateBundleFunc: syncer.createLocalPolicySpecBundleFunc,
		Predicate:        predicate,
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPlacementRulesMsgKey,
		CreateBundleFunc: syncer.createLocalPlacementRuleSpecBundleFunc,
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
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return LocalGenericHandleBundle(ctx, bundle, db.LocalSpecSchema, db.LocalPolicySpecTableName, dbClient,
				syncer.log)
		}))

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPlacementRuleSpecPriority,
		helpers.GetBundleType(syncer.createLocalPlacementRuleSpecBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return LocalGenericHandleBundle(ctx, bundle, db.LocalSpecSchema, db.LocalPlacementRuleTableName, dbClient,
				syncer.log)
		}))
}
