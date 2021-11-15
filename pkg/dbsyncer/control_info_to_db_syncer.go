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

// NewControlInfoDBSyncer creates a new instance of ControlInfoTransportToDBSyncer.
func NewControlInfoDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &ControlInfoTransportToDBSyncer{
		log:              log,
		createBundleFunc: func() bundle.Bundle { return bundle.NewControlInfoStatusBundle() },
	}

	log.Info("initialized control info db syncer")

	return dbSyncer
}

// ControlInfoTransportToDBSyncer implements control info transport to db sync.
type ControlInfoTransportToDBSyncer struct {
	log              logr.Logger
	createBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *ControlInfoTransportToDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.ControlInfoMsgKey,
		CreateBundleFunc: syncer.createBundleFunc,
		Predicate:        func() bool { return true }, // always get control info bundles
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the conflation manager.
func (syncer *ControlInfoTransportToDBSyncer) RegisterBundleHandlerFunctions(
	conflationManager *conflator.ConflationManager) {
	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.ControlInfoPriority,
		helpers.GetBundleType(syncer.createBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			if err := dbClient.UpdateHeartbeat(ctx, db.LeafHubHeartbeatsTableName, bundle.GetLeafHubName()); err != nil {
				syncer.log.Error(err, "failed to handle bundle")

				return fmt.Errorf("failed to perform batch - %w", err)
			}

			return nil
		},
	))
}
