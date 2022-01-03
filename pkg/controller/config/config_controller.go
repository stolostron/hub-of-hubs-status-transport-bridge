package config

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	requeuePeriodSeconds = 5
	configName           = "hub-of-hubs-config"
)

// AddConfigController creates a new instance of config controller and adds it to the manager.
func AddConfigController(mgr ctrl.Manager, log logr.Logger, config *configv1.Config) error {
	hubOfHubsConfigCtrl := &hubOfHubsConfigController{
		client: mgr.GetClient(),
		log:    log,
		config: config,
	}

	hohNamespacePredicate := predicate.NewPredicateFuncs(func(object client.Object) bool {
		return object.GetNamespace() == datatypes.HohSystemNamespace && object.GetName() == configName
	})

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&configv1.Config{}).
		WithEventFilter(hohNamespacePredicate).
		Complete(hubOfHubsConfigCtrl); err != nil {
		return fmt.Errorf("failed to add config controller to manager - %w", err)
	}

	if err := mgr.GetAPIReader().Get(context.Background(), client.ObjectKey{
		Namespace: datatypes.HohSystemNamespace,
		Name:      configName,
	}, config); err != nil {
		return fmt.Errorf("failed to read Config - %w", err)
	}

	return nil
}

type hubOfHubsConfigController struct {
	client client.Client
	log    logr.Logger
	config *configv1.Config
}

func (c *hubOfHubsConfigController) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	reqLogger := c.log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)

	if err := c.client.Get(ctx, request.NamespacedName, c.config); apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	} else if err != nil {
		reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
		return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second},
			fmt.Errorf("reconciliation failed: %w", err)
	}

	reqLogger.Info("Reconciliation complete.")

	return ctrl.Result{}, nil
}
