package dbsyncer

import (
	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewPlacementDecisionsDBSyncer creates a new instance of genericDBSyncer to sync placement-decisions.
func NewPlacementDecisionsDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &genericDBSyncer{
		log:              log,
		transportMsgKey:  datatypes.PlacementDecisionMsgKey,
		dbSchema:         db.StatusSchema,
		dbTableName:      db.PlacementDecisionsTableName,
		createBundleFunc: bundle.NewPlacementDecisionsBundle,
		bundlePriority:   conflator.PlacementDecisionPriority,
		bundleSyncMode:   status.CompleteStateMode,
	}

	log.Info("initialized placement-decisions db syncer")

	return dbSyncer
}
