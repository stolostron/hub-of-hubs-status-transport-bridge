package controller

import (
	"fmt"

	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	configCtrl "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/config"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/dbsyncer"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const aggregationLevelUnset = "Unset"

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

// AddSyncers adds the controllers to sync info from transport to DB to the Manager.
func AddSyncers(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool, statusTransport transport.Transport) error {
	addDBSyncerFunctions := []func(ctrl.Manager, *workerpool.DBWorkerPool, transport.Transport,
		*configv1.Config) error{
		addClustersTransportToDBSyncer, addPoliciesTransportToDBSyncer, addControlInfoTransportToDBSyncer,
	}

	config := &configv1.Config{}
	config.Spec.AggregationLevel = aggregationLevelUnset

	if err := configCtrl.AddConfigController(mgr, "hub-of-hubs-config", config); err != nil {
		return fmt.Errorf("failed to add controller: %w", err)
	}

	for _, addDBSyncerFunction := range addDBSyncerFunctions {
		if err := addDBSyncerFunction(mgr, dbWorkerPool, statusTransport, config); err != nil {
			return fmt.Errorf("failed to add DB Syncer: %w", err)
		}
	}

	return nil
}

func addClustersTransportToDBSyncer(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool,
	statusTransport transport.Transport, _ *configv1.Config) error {
	err := dbsyncer.AddClustersTransportToDBSyncer(
		mgr,
		ctrl.Log.WithName("managed-clusters-transport-to-db-syncer"),
		dbWorkerPool,
		statusTransport,
		dbsyncer.ManagedClustersTableName,
		&transport.BundleRegistration{
			MsgID:            datatypes.ManagedClustersMsgKey,
			CreateBundleFunc: func() bundle.Bundle { return bundle.NewManagedClustersStatusBundle() },
			Predicate:        func() bool { return true }, // always get managed clusters bundles
		})
	if err != nil {
		return fmt.Errorf("failed to add DB Syncer: %w", err)
	}

	return nil
}

func addPoliciesTransportToDBSyncer(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool,
	statusTransport transport.Transport, config *configv1.Config) error {
	fullStatusPredicate := func() bool {
		return config.Spec.AggregationLevel == configv1.Full || config.Spec.AggregationLevel == aggregationLevelUnset
	}
	minimalStatusPredicate := func() bool {
		return config.Spec.AggregationLevel == configv1.Minimal || config.Spec.AggregationLevel == aggregationLevelUnset
	}

	err := dbsyncer.AddPoliciesTransportToDBSyncer(
		mgr,
		ctrl.Log.WithName("policies-transport-to-db-syncer"),
		dbWorkerPool,
		statusTransport,
		dbsyncer.ManagedClustersTableName,
		dbsyncer.ComplianceTableName,
		dbsyncer.MinimalComplianceTableName,
		&transport.BundleRegistration{
			MsgID:            datatypes.ClustersPerPolicyMsgKey,
			CreateBundleFunc: func() bundle.Bundle { return bundle.NewClustersPerPolicyBundle() },
			Predicate:        fullStatusPredicate,
		},
		&transport.BundleRegistration{
			MsgID:            datatypes.PolicyComplianceMsgKey,
			CreateBundleFunc: func() bundle.Bundle { return bundle.NewComplianceStatusBundle() },
			Predicate:        fullStatusPredicate,
		},
		&transport.BundleRegistration{
			MsgID:            datatypes.MinimalPolicyComplianceMsgKey,
			CreateBundleFunc: func() bundle.Bundle { return bundle.NewMinimalComplianceStatusBundle() },
			Predicate:        minimalStatusPredicate,
		})
	if err != nil {
		return fmt.Errorf("failed to add DB Syncer: %w", err)
	}

	return nil
}

func addControlInfoTransportToDBSyncer(mgr ctrl.Manager, dbWorkerPool *workerpool.DBWorkerPool,
	statusTransport transport.Transport, _ *configv1.Config) error {
	err := dbsyncer.AddControlInfoTransportToDBSyncer(
		mgr,
		ctrl.Log.WithName("control-info-transport-to-db-syncer"),
		dbWorkerPool,
		statusTransport,
		dbsyncer.LeafHubHeartbeatsTableName,
		&transport.BundleRegistration{
			MsgID:            datatypes.ControlInfoKey,
			CreateBundleFunc: func() bundle.Bundle { return bundle.NewControlInfoStatusBundle() },
			Predicate:        func() bool { return true }, // always get control info bundles
		})
	if err != nil {
		return fmt.Errorf("failed to add DB Syncer: %w", err)
	}

	return nil
}
