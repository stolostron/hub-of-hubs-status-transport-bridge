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
)

// NewControlInfoDBSyncer creates a new instance of ControlInfoDBSyncer.
func NewControlInfoDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &ControlInfoDBSyncer{
		log:              log,
		createBundleFunc: bundle.NewControlInfoBundle,
	}

	log.Info("initialized control info db syncer")

	return dbSyncer
}

// ControlInfoDBSyncer implements control info transport to db sync.
type ControlInfoDBSyncer struct {
	log              logr.Logger
	createBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *ControlInfoDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.ControlInfoMsgKey,
		CreateBundleFunc: syncer.createBundleFunc,
		Predicate:        func() bool { return true }, // always get control info bundles
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the conflation manager.
func (syncer *ControlInfoDBSyncer) RegisterBundleHandlerFunctions(
	conflationManager *conflator.ConflationManager) {
	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.ControlInfoPriority,
		helpers.GetBundleType(syncer.createBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleControlInfoBundle(ctx, bundle, dbClient)
		},
	))
}

func (syncer *ControlInfoDBSyncer) handleControlInfoBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.ControlInfoDB) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)

	if err := dbClient.UpdateHeartbeat(ctx, db.StatusSchema, db.LeafHubHeartbeatsTableName,
		bundle.GetLeafHubName()); err != nil {
		return fmt.Errorf("failed to handle control info bundle - %w", err)
	}

	logBundleHandlingMessage(syncer.log, bundle, finishBundleHandlingMessage)

	return nil
}
