package dbsyncer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	envVarDBSyncInterval = "DB_SYNC_INTERVAL"
)

// AddDBConfigHandler adds db config handler to the manager.
func AddDBConfigHandler(mgr ctrl.Manager, log logr.Logger, db hohDb.ManageStatusDB, config *configv1.Config) error {
	syncIntervalString, found := os.LookupEnv(envVarDBSyncInterval)
	if !found {
		return fmt.Errorf("environment variable %s not found", envVarDBSyncInterval)
	}

	syncInterval, err := time.ParseDuration(syncIntervalString)
	if err != nil {
		return fmt.Errorf("environment variable %s is not valid duration", envVarDBSyncInterval)
	}

	handler := &DBConfigHandler{
		log:          log,
		db:           db,
		config:       config,
		syncInterval: syncInterval,
	}

	log.Info("initialized db config handler")

	return mgr.Add(handler)
}

// DBConfigHandler is a handler that manages db tables according to the current hub of hubs config
type DBConfigHandler struct {
	log          logr.Logger
	db           hohDb.ManageStatusDB
	config       *configv1.Config
	syncInterval time.Duration
}

// Start function starts the db handler.
func (handler *DBConfigHandler) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	handler.periodicDBConfigHandling(ctx) // run same periodic logic on initialization
	ticker := time.NewTicker(handler.syncInterval)

	for {
		select {
		case <-stopChannel:
			ticker.Stop()

			handler.log.Info("stopped db config handler")
			cancelContext()

			return nil
		case <-ticker.C:
			go handler.periodicDBConfigHandling(ctx)
		}
	}
}

func (handler *DBConfigHandler) periodicDBConfigHandling(ctx context.Context) {
	if handler.config.Spec.AggregationLevel == configv1.Full {
		if err := handler.db.DeleteTableContent(ctx, MinimalComplianceTableName); err != nil {
			handler.log.Error(err, "failed deleting content of table '%s'", MinimalComplianceTableName)
		}
	} else if handler.config.Spec.AggregationLevel == configv1.Minimal {
		if err := handler.db.DeleteTableContent(ctx, ComplianceTableName); err != nil {
			handler.log.Error(err, "failed deleting content of table '%s'", ComplianceTableName)
		}
	}
}
