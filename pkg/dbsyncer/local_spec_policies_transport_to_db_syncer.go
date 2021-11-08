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
		Predicate:        func() bool { return syncer.config.Spec.EnableLocalPolicies },
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.LocalPolicySpecMsgKey,
		CreateBundleFunc: syncer.createLocalPolicySpecBundleFunc,
		Predicate:        func() bool { return syncer.config.Spec.EnableLocalPolicies },
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
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return GenericHandleBundle(ctx, bundle, db.LocalSpecSchema, db.LocalPolicySpecTableName, dbClient,
				syncer.log)
		}))

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.LocalPlacementRuleSpecPriority,
		localPlacementRuleSpecBundleType,
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return GenericHandleBundle(ctx, bundle, db.LocalSpecSchema, db.LocalPlacementRuleTableName, dbClient,
				syncer.log)
		}))
}
