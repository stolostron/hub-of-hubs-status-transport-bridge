package dbsyncer

import (
	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewPlacementsDBSyncer creates a new instance of genericDBSyncer to sync placements.
func NewPlacementsDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &genericDBSyncer{
		log:              log,
		transportMsgKey:  datatypes.PlacementMsgKey,
		dbSchema:         db.StatusSchema,
		dbTableName:      db.PlacementsTableName,
		createBundleFunc: bundle.NewPlacementsBundle,
		bundlePriority:   conflator.PlacementPriority,
		bundleSyncMode:   status.CompleteStateMode,
	}

	log.Info("initialized placements db syncer")

	return dbSyncer
}
