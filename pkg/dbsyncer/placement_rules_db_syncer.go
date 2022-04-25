package dbsyncer

import (
	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewPlacementRulesDBSyncer creates a new instance of genericDBSyncer to sync placement-rules.
func NewPlacementRulesDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &genericDBSyncer{
		log:              log,
		transportMsgKey:  datatypes.PlacementRuleMsgKey,
		dbSchema:         db.StatusSchema,
		dbTableName:      db.PlacementRulesTableName,
		createBundleFunc: bundle.NewPlacementRulesBundle,
		bundlePriority:   conflator.PlacementRulePriority,
		bundleSyncMode:   status.CompleteStateMode,
	}

	log.Info("initialized placement-rules db syncer")

	return dbSyncer
}
