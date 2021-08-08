package config

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/predicate"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	requeuePeriodSeconds = 5
)

// AddConfigController creates a new instance of config controller and adds it to the manager.
func AddConfigController(mgr ctrl.Manager, logName string, configObject *configv1.Config) error {
	hubOfHubsConfigCtrl := &hubOfHubsConfigController{
		client:       mgr.GetClient(),
		log:          ctrl.Log.WithName(logName),
		configObject: configObject,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&configv1.Config{}).
		WithEventFilter(predicate.HoHNamespacePredicate).
		Complete(hubOfHubsConfigCtrl)
}

type hubOfHubsConfigController struct {
	client       client.Client
	log          logr.Logger
	configObject *configv1.Config
}

func (c *hubOfHubsConfigController) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	reqLogger := c.log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)

	ctx := context.Background()

	if err := c.client.Get(ctx, request.NamespacedName, c.configObject); apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	} else if err != nil {
		reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
		return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second}, err
	}

	reqLogger.Info("Reconciliation complete.")
	return ctrl.Result{}, nil
}
