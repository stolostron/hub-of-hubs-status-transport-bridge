package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/dbsyncer"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	requeuePeriodSeconds = 5
	envVarDBSyncInterval = "DB_SYNC_INTERVAL"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// AddConfigController creates a new instance of config controller and adds it to the manager.
func AddConfigController(mgr ctrl.Manager, logName string, db hohDb.ManageStatusDB, config *configv1.Config) error {
	syncIntervalString, found := os.LookupEnv(envVarDBSyncInterval)
	if !found {
		return fmt.Errorf("%w: %s", errEnvVarNotFound, envVarDBSyncInterval)
	}

	syncInterval, err := time.ParseDuration(syncIntervalString)
	if err != nil {
		return fmt.Errorf("environment variable %s is not valid duration - %w", envVarDBSyncInterval, err)
	}

	hubOfHubsConfigCtrl := &hubOfHubsConfigController{
		client:       mgr.GetClient(),
		log:          ctrl.Log.WithName(logName),
		config:       config,
		db:           db,
		syncInterval: syncInterval,
	}

	go hubOfHubsConfigCtrl.periodicSync()

	hohNamespacePredicate := predicate.NewPredicateFuncs(func(meta metav1.Object, object runtime.Object) bool {
		return meta.GetNamespace() == datatypes.HohSystemNamespace
	})

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&configv1.Config{}).
		WithEventFilter(hohNamespacePredicate).
		Complete(hubOfHubsConfigCtrl); err != nil {
		return fmt.Errorf("failed to add config controller to manager - %w", err)
	}

	return nil
}

type hubOfHubsConfigController struct {
	client       client.Client
	log          logr.Logger
	config       *configv1.Config
	db           hohDb.ManageStatusDB
	syncInterval time.Duration
}

func (c *hubOfHubsConfigController) periodicSync() {
	ctx := context.Background()
	ticker := time.NewTicker(c.syncInterval)

	for {
		<-ticker.C

		if err := c.handleConfig(ctx); err != nil {
			c.log.Error(err, "failed in periodic config handling")
		}
	}
}

func (c *hubOfHubsConfigController) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	reqLogger := c.log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)

	ctx := context.Background()

	if err := c.client.Get(ctx, request.NamespacedName, c.config); apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	} else if err != nil {
		reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
		return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second},
			fmt.Errorf("reconciliation failed: %w", err)
	}

	if err := c.handleConfig(ctx); err != nil {
		reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
		return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second},
			fmt.Errorf("reconciliation failed: %w", err)
	}

	reqLogger.Info("Reconciliation complete.")

	return ctrl.Result{}, nil
}

func (c *hubOfHubsConfigController) handleConfig(ctx context.Context) error {
	if c.config.Spec.AggregationLevel == configv1.Full {
		if err := c.db.DeleteTableContent(ctx, dbsyncer.MinimalComplianceTableName); err != nil {
			return fmt.Errorf("failed deleting content of table '%s' - %w", dbsyncer.MinimalComplianceTableName,
				err)
		}
	} else if c.config.Spec.AggregationLevel == configv1.Minimal {
		if err := c.db.DeleteTableContent(ctx, dbsyncer.ComplianceTableName); err != nil {
			return fmt.Errorf("failed deleting content of table '%s' - %w", dbsyncer.ComplianceTableName, err)
		}
	}

	return nil
}
