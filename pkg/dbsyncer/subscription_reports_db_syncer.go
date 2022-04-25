package dbsyncer

import (
	"github.com/go-logr/logr"
	datatypes "github.com/stolostron/hub-of-hubs-data-types"
	"github.com/stolostron/hub-of-hubs-data-types/bundle/status"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewSubscriptionReportsDBSyncer creates a new instance of genericDBSyncer to sync subscription-reports.
func NewSubscriptionReportsDBSyncer(log logr.Logger) DBSyncer {
	dbSyncer := &genericDBSyncer{
		log:              log,
		transportMsgKey:  datatypes.SubscriptionReportMsgKey,
		dbSchema:         db.StatusSchema,
		dbTableName:      db.SubscriptionReportsTableName,
		createBundleFunc: bundle.NewSubscriptionReportsBundle,
		bundlePriority:   conflator.SubscriptionReportPriority,
		bundleSyncMode:   status.CompleteStateMode,
	}

	log.Info("initialized subscription-reports db syncer")

	return dbSyncer
}
