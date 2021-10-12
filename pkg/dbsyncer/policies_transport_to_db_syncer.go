package dbsyncer

import (
	"context"
	"fmt"

	set "github.com/deckarep/golang-set"
	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// NewPoliciesDBSyncer creates a new instance of PoliciesDBSyncer.
func NewPoliciesDBSyncer(log logr.Logger, config *configv1.Config) DBSyncer {
	dbSyncer := &PoliciesDBSyncer{
		log:                                 log,
		config:                              config,
		createClustersPerPolicyBundleFunc:   func() bundle.Bundle { return bundle.NewClustersPerPolicyBundle() },
		createComplianceStatusBundleFunc:    func() bundle.Bundle { return bundle.NewComplianceStatusBundle() },
		createMinComplianceStatusBundleFunc: func() bundle.Bundle { return bundle.NewMinimalComplianceStatusBundle() },
	}

	log.Info("initialized policies db syncer")

	return dbSyncer
}

// PoliciesDBSyncer implements policies db sync business logic.
type PoliciesDBSyncer struct {
	log                                 logr.Logger
	config                              *configv1.Config
	createClustersPerPolicyBundleFunc   func() bundle.Bundle
	createComplianceStatusBundleFunc    func() bundle.Bundle
	createMinComplianceStatusBundleFunc func() bundle.Bundle
}

// RegisterCreateBundleFunctions registers create bundle functions within the transport instance.
func (syncer *PoliciesDBSyncer) RegisterCreateBundleFunctions(transportInstance transport.Transport) {
	fullStatusPredicate := func() bool { return syncer.config.Spec.AggregationLevel == configv1.Full }
	minimalStatusPredicate := func() bool { return syncer.config.Spec.AggregationLevel == configv1.Minimal }

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.ClustersPerPolicyMsgKey,
		CreateBundleFunc: syncer.createClustersPerPolicyBundleFunc,
		Predicate:        fullStatusPredicate,
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.PolicyComplianceMsgKey,
		CreateBundleFunc: syncer.createComplianceStatusBundleFunc,
		Predicate:        fullStatusPredicate,
	})

	transportInstance.Register(&transport.BundleRegistration{
		MsgID:            datatypes.MinimalPolicyComplianceMsgKey,
		CreateBundleFunc: syncer.createMinComplianceStatusBundleFunc,
		Predicate:        minimalStatusPredicate,
	})
}

// RegisterBundleHandlerFunctions registers bundle handler functions within the dispatcher.
// handler functions need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *PoliciesDBSyncer) RegisterBundleHandlerFunctions(conflationManager *conflator.ConflationManager) {
	clustersPerPolicyBundleType := helpers.GetBundleType(syncer.createClustersPerPolicyBundleFunc())

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.ClustersPerPolicyPriority,
		clustersPerPolicyBundleType,
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleClustersPerPolicyBundle(ctx, bundle, dbClient, db.StatusSchema, db.ComplianceTable)
		},
	))

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.ComplianceStatusPriority,
		helpers.GetBundleType(syncer.createComplianceStatusBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleComplianceBundle(ctx, bundle, dbClient, db.StatusSchema, db.ComplianceTable)
		}).WithDependency(dependency.NewDependency(clustersPerPolicyBundleType)), // comp depends on clusters per policy
	)

	conflationManager.Register(conflator.NewConflationRegistration(
		conflator.MinimalComplianceStatusPriority,
		helpers.GetBundleType(syncer.createMinComplianceStatusBundleFunc()),
		func(ctx context.Context, bundle bundle.Bundle, dbClient db.StatusTransportBridgeDB) error {
			return syncer.handleMinimalComplianceBundle(ctx, bundle, dbClient)
		},
	))
}

// if we got inside the handler function, then the bundle generation is newer than what was already handled.
// handling clusters per policy bundle only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function.
func (syncer *PoliciesDBSyncer) handleClustersPerPolicyBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.PoliciesStatusDB, dbSchema string, dbTableName string) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	complianceRowsFromDB, err := dbClient.GetComplianceStatusByLeafHub(ctx, dbSchema, dbTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' compliance status rows from db - %w", leafHubName, err)
	}

	batchBuilder := dbClient.NewPoliciesBatchBuilder(dbSchema, dbTableName, leafHubName)

	for _, object := range bundle.GetObjects() { // every object is clusters list per policy
		clustersPerPolicy, ok := object.(*statusbundle.ClustersPerPolicy)
		if !ok {
			continue // do not handle objects other than ClustersPerPolicy
		}

		clustersFromDB, policyExistsInDB := complianceRowsFromDB[clustersPerPolicy.PolicyID]
		if !policyExistsInDB {
			clustersFromDB = set.NewSet()
		}

		syncer.handleClusterPerPolicy(batchBuilder, clustersPerPolicy, clustersFromDB)

		// keep this policy in db, should remove from db only policies that were not sent in the bundle
		delete(complianceRowsFromDB, clustersPerPolicy.PolicyID)
	}
	// remove policies that were not sent in the bundle
	for policyID := range complianceRowsFromDB {
		batchBuilder.DeletePolicy(policyID)
	}
	// batch may contain up to the number of compliance status rows per leaf hub, that is (num_of_policies * num_of_MCs)
	if err := dbClient.SendBatch(ctx, batchBuilder.Build()); err != nil {
		return fmt.Errorf("failed to perform batch - %w", err)
	}

	logBundleHandlingMessage(syncer.log, bundle, finishBundleHandlingMessage)

	return nil
}

func (syncer *PoliciesDBSyncer) handleClusterPerPolicy(batchBuilder db.PoliciesBatchBuilder,
	clustersFromBundle *statusbundle.ClustersPerPolicy, clustersFromDB set.Set) {
	for _, clusterName := range clustersFromBundle.Clusters { // go over the clusters per policy from bundle
		if !clustersFromDB.Contains(clusterName) { // check if cluster not found in the db compliance table
			batchBuilder.Insert(clustersFromBundle.PolicyID, clusterName, db.ErrorNone, db.Unknown)
			continue
		}
		// compliance row exists both in db and in the bundle. remove from clustersFromDB since
		// we don't update the rows in clusters per policy bundle, only insert new compliance rows or delete non
		// relevant rows
		clustersFromDB.Remove(clusterName)
	}
	// delete compliance status rows that in the db but not sent in the bundle (leaf hub sends only living resources)
	clustersFromDB.Each(func(object interface{}) bool {
		clusterName, ok := object.(string)
		if !ok {
			return false // if object is not a cluster name string ,do nothing.
		}

		batchBuilder.DeleteClusterStatus(clustersFromBundle.PolicyID, clusterName)

		return false // return true with this set implementation will stop the iteration, therefore need to return false
	})
}

// if we got the the handler function, then the bundle pre-conditions were satisfied (the generation is newer than what
// was already handled and base bundle was already handled successfully)
// we assume that 'ClustersPerPolicy' handler function handles the addition or removal of clusters rows.
// in this handler function, we handle only the existing clusters rows.
func (syncer *PoliciesDBSyncer) handleComplianceBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.PoliciesStatusDB, dbSchema string, dbTableName string) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	nonCompliantRowsFromDB, err := dbClient.GetNonCompliantClustersByLeafHub(ctx, dbSchema, dbTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' compliance status rows from db - %w", leafHubName, err)
	}

	batchBuilder := dbClient.NewPoliciesBatchBuilder(dbSchema, dbTableName, leafHubName)

	for _, object := range bundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus, ok := object.(*statusbundle.PolicyComplianceStatus)
		if !ok {
			continue // do not handle objects other than PolicyComplianceStatus
		}
		// nonCompliantClusters includes both non Compliant and Unknown clusters
		nonCompliantClustersFromDB, policyExistsInDB := nonCompliantRowsFromDB[policyComplianceStatus.PolicyID]
		if !policyExistsInDB {
			nonCompliantClustersFromDB = db.NewNonCompliantClusterSets()
		}

		syncer.handlePolicyComplianceStatus(batchBuilder, policyComplianceStatus,
			nonCompliantClustersFromDB.GetNonCompliantClusters(), nonCompliantClustersFromDB.GetUnknownClusters())

		// for policies that are found in the db but not in the bundle - all clusters are Compliant (implicitly)
		delete(nonCompliantRowsFromDB, policyComplianceStatus.PolicyID)
	}
	// update policies not in the bundle - all is Compliant
	for policyID := range nonCompliantRowsFromDB {
		batchBuilder.UpdatePolicyCompliance(policyID, db.Compliant)
	}
	// batch may contain up to the number of compliance status rows per leaf hub, that is (num_of_policies * num_of_MCs)
	if err := dbClient.SendBatch(ctx, batchBuilder.Build()); err != nil {
		return fmt.Errorf("failed to perform batch - %w", err)
	}

	logBundleHandlingMessage(syncer.log, bundle, finishBundleHandlingMessage)

	return nil
}

func (syncer *PoliciesDBSyncer) handlePolicyComplianceStatus(batchBuilder db.PoliciesBatchBuilder,
	policyComplianceStatus *statusbundle.PolicyComplianceStatus, nonCompliantClustersFromDB set.Set,
	unknownClustersFromDB set.Set) {
	// put non compliant and unknown clusters in a single set.
	// the clusters that will be left in this set, will be considered implicitly as compliant
	clustersFromDB := nonCompliantClustersFromDB.Union(unknownClustersFromDB)

	// update in db batch the non Compliant clusters as it was reported by leaf hub
	for _, clusterName := range policyComplianceStatus.NonCompliantClusters { // go over bundle non compliant clusters
		if !nonCompliantClustersFromDB.Contains(clusterName) { // check if row is different than non compliant in db
			batchBuilder.UpdateClusterCompliance(policyComplianceStatus.PolicyID, clusterName, db.NonCompliant)
		} // if different need to update, otherwise no need to do anything.

		clustersFromDB.Remove(clusterName) // mark cluster as handled
	}
	// update in db batch the unknown clusters as it was reported by leaf hub
	for _, clusterName := range policyComplianceStatus.UnknownComplianceClusters { // go over bundle unknown clusters
		if !unknownClustersFromDB.Contains(clusterName) { // check if row is different than unknown in db
			batchBuilder.UpdateClusterCompliance(policyComplianceStatus.PolicyID, clusterName, db.Unknown)
		} // if different need to update, otherwise no need to do anything.

		clustersFromDB.Remove(clusterName) // mark cluster as handled
	}
	// clusters left in the union of non compliant + unknown clusters from db are implicitly considered as Compliant
	clustersFromDB.Each(func(object interface{}) bool {
		clusterName, ok := object.(string)
		if !ok {
			return false // if object is not a cluster name string ,do nothing.
		}
		// change to Compliant
		batchBuilder.UpdateClusterCompliance(policyComplianceStatus.PolicyID, clusterName, db.Compliant)

		return false // return true with this set implementation will stop the iteration, therefore need to return false
	})
}

// if we got the the handler function, then the bundle pre-conditions are satisfied.
func (syncer *PoliciesDBSyncer) handleMinimalComplianceBundle(ctx context.Context, bundle bundle.Bundle,
	dbClient db.AggregatedPoliciesStatusDB) error {
	logBundleHandlingMessage(syncer.log, bundle, startBundleHandlingMessage)
	leafHubName := bundle.GetLeafHubName()

	policyIDsFromDB, err := dbClient.GetPolicyIDsByLeafHub(ctx, db.StatusSchema, db.MinimalComplianceTable, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is minimal policy compliance status.
		minPolicyCompliance, ok := object.(*statusbundle.MinimalPolicyComplianceStatus)
		if !ok {
			continue // do not handle objects other than MinimalPolicyComplianceStatus.
		}

		if err := dbClient.InsertOrUpdateAggregatedPolicyCompliance(ctx, db.StatusSchema, db.MinimalComplianceTable,
			leafHubName, minPolicyCompliance.PolicyID, minPolicyCompliance.AppliedClusters,
			minPolicyCompliance.NonCompliantClusters); err != nil {
			return fmt.Errorf("failed to update minimal compliance of policy '%s', leaf hub '%s' in db - %w",
				minPolicyCompliance.PolicyID, leafHubName, err)
		}
		// policy that is found both in db and bundle, need to remove from policiesFromDB
		// eventually we will be left with policies not in the bundle inside policyIDsFromDB and will use it to remove
		// policies that has to be deleted from the table.
		policyIDsFromDB.Remove(minPolicyCompliance.PolicyID)
	}

	// remove policies that in the db but were not sent in the bundle (leaf hub sends only living resources).
	for _, object := range policyIDsFromDB.ToSlice() {
		policyID, ok := object.(string)
		if !ok {
			continue
		}

		if err := dbClient.DeleteAllComplianceRows(ctx, db.StatusSchema, db.MinimalComplianceTable, leafHubName,
			policyID); err != nil {
			return fmt.Errorf("failed deleted compliance rows of policy '%s', leaf hub '%s' from db - %w",
				policyID, leafHubName, err)
		}
	}

	logBundleHandlingMessage(syncer.log, bundle, finishBundleHandlingMessage)

	return nil
}
