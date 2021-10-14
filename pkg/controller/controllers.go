package controller

import (
	"fmt"

	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	configCtrl "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/config"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/dispatcher"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/dbsyncer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// AddToScheme adds all the resources to be processed to the Scheme.
func AddToScheme(s *runtime.Scheme) error {
	schemeBuilders := []*scheme.Builder{configv1.SchemeBuilder}

	for _, schemeBuilder := range schemeBuilders {
		if err := schemeBuilder.AddToScheme(s); err != nil {
			return fmt.Errorf("failed to add scheme: %w", err)
		}
	}

	return nil
}

// Setup performs the initial setup required before starting the runtime manager.
// adds controllers and/or runnables to the manager, registers handler functions within the dispatcher and create bundle
// functions within the transport.
func Setup(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool, conflationManager *conflator.ConflationManager,
	conflationReadyQueue *conflator.ConflationReadyQueue, transport transport.Transport,
	statistics manager.Runnable) error {
	// register config controller within the runtime manager
	config, err := addConfigController(mgr)
	if err != nil {
		return fmt.Errorf("failed to add config controller to manager - %w", err)
	}
	// register statistics within the runtime manager
	if err := mgr.Add(statistics); err != nil {
		return fmt.Errorf("failed to add statistics to manager - %w", err)
	}
	// register dispatcher within the runtime manager
	if err := addDispatcher(mgr, dbWorkerPool, conflationReadyQueue); err != nil {
		return fmt.Errorf("failed to add dispatcher to manager - %w", err)
	}
	// register db syncers create bundle functions within transport and handler functions within dispatcher
	dbSyncers := []dbsyncer.DBSyncer{
		dbsyncer.NewManagedClustersDBSyncer(ctrl.Log.WithName("managed clusters db syncer")),
		dbsyncer.NewPoliciesDBSyncer(ctrl.Log.WithName("policies db syncer"), config),
	}

	for _, dbsyncerObj := range dbSyncers {
		dbsyncerObj.RegisterCreateBundleFunctions(transport)
		dbsyncerObj.RegisterBundleHandlerFunctions(conflationManager)
	}

	return nil
}

func addConfigController(mgr ctrl.Manager) (*configv1.Config, error) {
	config := &configv1.Config{}
	config.Spec.AggregationLevel = configv1.Full // default value is full until the config is read from the CR

	if err := configCtrl.AddConfigController(mgr, "hub-of-hubs-config", config); err != nil {
		return nil, fmt.Errorf("failed to add controller: %w", err)
	}

	return config, nil
}

func addDispatcher(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool,
	conflationReadyQueue *conflator.ConflationReadyQueue) error {
	if err := mgr.Add(dispatcher.NewDispatcher(
		ctrl.Log.WithName("dispatcher"),
		conflationReadyQueue,
		dbWorkerPool,
	)); err != nil {
		return fmt.Errorf("failed to add dispatcher: %w", err)
	}

	return nil
}
