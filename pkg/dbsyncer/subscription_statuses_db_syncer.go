package dbsyncer

import (
	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewSubscriptionStatusesDBSyncer creates a new instance of genericDBSyncer to sync subscription-statuses.
func NewSubscriptionStatusesDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &genericDBSyncer{
		log:              log,
		transportMsgKey:  datatypes.SubscriptionStatusMsgKey,
		dbSchema:         db.StatusSchema,
		dbTableName:      db.SubscriptionStatusesTableName,
		createBundleFunc: bundle.NewSubscriptionStatusesBundle,
		bundlePriority:   conflator.SubscriptionStatusPriority,
		bundleSyncMode:   status.CompleteStateMode,
	}

	log.Info("initialized subscription-statuses db syncer")

	return dbSyncer
}
