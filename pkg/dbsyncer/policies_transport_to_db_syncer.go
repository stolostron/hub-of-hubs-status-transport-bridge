package dbsyncer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	configv1 "github.com/open-cluster-management/hub-of-hubs-data-types/apis/config/v1"
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/dispatcher"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

const (
	errorNone = "none"

	nonCompliant = "non_compliant"
	compliant    = "compliant"
	unknown      = "unknown"

	inform  = "inform"
	enforce = "enforce"
)

var errFailedToHandleClustersPerPolicy = errors.New("failed to handle 'ClustersPerPolicy'")

// NewPoliciesDBSyncer creates a new instance of PoliciesDBSyncer.
func NewPoliciesDBSyncer(log logr.Logger, config *configv1.Config) DBSyncer {
	dbSyncer := &PoliciesDBSyncer{
		log:                                 log,
		config:                              config,
		createClustersPerPolicyBundleFunc:   func() bundle.Bundle { return bundle.NewClustersPerPolicyBundle() },
		createComplianceStatusBundleFunc:    func() bundle.Bundle { return bundle.NewComplianceStatusBundle() },
		createMinComplianceStatusBundleFunc: func() bundle.Bundle { return bundle.NewMinimalComplianceStatusBundle() },
	}

	log.Info("initialized policies syncer")

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
func (syncer *PoliciesDBSyncer) RegisterBundleHandlerFunctions(dispatcher *dispatcher.Dispatcher) {
	dispatcher.RegisterHandler(helpers.GetBundleType(syncer.createClustersPerPolicyBundleFunc()),
		syncer.handleClustersPerPolicyBundle)
	dispatcher.RegisterHandler(helpers.GetBundleType(syncer.createComplianceStatusBundleFunc()),
		syncer.handleComplianceBundle)
	dispatcher.RegisterHandler(helpers.GetBundleType(syncer.createMinComplianceStatusBundleFunc()),
		syncer.handleMinimalComplianceBundle)
}

// if we got inside the handler function, then the bundle generation is newer than what was already handled.
// handling clusters per policy bundle only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function.
func (syncer *PoliciesDBSyncer) handleClustersPerPolicyBundle(ctx context.Context, bundle bundle.Bundle,
	dbConn db.StatusTransportBridgeDB) error {
	leafHubName := bundle.GetLeafHubName()
	bundleGeneration := bundle.GetGeneration()

	syncer.log.Info("start handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGeneration)

	policyIDsFromDB, err := dbConn.GetPolicyIDsByLeafHub(ctx, complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object is clusters list + enforcement per policy
		clustersPerPolicy, ok := object.(*statusbundle.ClustersPerPolicy)
		if !ok {
			continue // do not handle objects other than ClustersPerPolicy
		}

		err := syncer.handleClusterPerPolicy(ctx, dbConn, leafHubName, bundleGeneration, clustersPerPolicy)
		if err != nil {
			return fmt.Errorf("failed handling clusters per policy bundle - %w", err)
		}

		// keep this policy in db, should remove from db policies that were not sent in the bundle
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, clustersPerPolicy.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...) // policy exists
		}
	}
	// remove policies that were not sent in the bundle
	if err := syncer.deletePoliciesFromDB(ctx, dbConn, leafHubName, policyIDsFromDB); err != nil {
		return fmt.Errorf("failed deleting policies from db - %w", err)
	}

	syncer.log.Info("finished handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGeneration)

	return nil
}

func (syncer *PoliciesDBSyncer) handleClusterPerPolicy(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	leafHubName string, bundleGeneration uint64, clustersPerPolicy *statusbundle.ClustersPerPolicy) error {
	clustersFromDB, err := dbConn.GetComplianceClustersByLeafHubAndPolicy(ctx, complianceTableName, leafHubName,
		clustersPerPolicy.PolicyID)
	if err != nil {
		return fmt.Errorf("failed to get clusters by leaf hub and policy from db - %w", err)
	}

	for _, clusterName := range clustersPerPolicy.Clusters { // go over the clusters per policy
		if !dbConn.ManagedClusterExists(ctx, managedClustersTableName, leafHubName, clusterName) {
			return fmt.Errorf(`%w from leaf hub '%s', generation %d - cluster '%s' doesn't exist`,
				errFailedToHandleClustersPerPolicy, leafHubName, bundleGeneration, clusterName)
		}

		clusterIndex, err := helpers.GetObjectIndex(clustersFromDB, clusterName)
		if err != nil { // cluster not found in the compliance table (and exists in managed clusters status table)
			if err = dbConn.InsertPolicyCompliance(ctx, complianceTableName, clustersPerPolicy.PolicyID, clusterName,
				leafHubName, errorNone, unknown, syncer.getEnforcement(clustersPerPolicy.RemediationAction),
				clustersPerPolicy.ResourceVersion); err != nil {
				return fmt.Errorf("failed to insert cluster '%s' from leaf hub '%s' compliance to DB - %w",
					clusterName, leafHubName, err)
			}

			continue
		}
		// compliance row exists both in db and in the bundle. remove from clustersFromDB since we don't update the
		// rows in clusters per policy bundle, only insert new compliance rows or delete non relevant rows
		clustersFromDB = append(clustersFromDB[:clusterIndex], clustersFromDB[clusterIndex+1:]...)
	}
	// delete compliance rows that in the db but were not sent in the bundle (leaf hub sends only living resources)
	err = syncer.deleteSelectedComplianceRows(ctx, dbConn, leafHubName, clustersPerPolicy.PolicyID, clustersFromDB)
	if err != nil {
		return fmt.Errorf("failed deleting compliance rows of policy '%s', leaf hub '%s' from db - %w",
			clustersPerPolicy.PolicyID, leafHubName, err)
	}
	// update enforcement and version of all rows with leafHub and policyId
	if err = dbConn.UpdateEnforcementAndResourceVersion(ctx, complianceTableName,
		clustersPerPolicy.PolicyID, leafHubName, syncer.getEnforcement(clustersPerPolicy.RemediationAction),
		clustersPerPolicy.ResourceVersion); err != nil {
		return fmt.Errorf(`failed updating enforcement and resource version of policy '%s', leaf hub '%s' 
					in db - %w`, clustersPerPolicy.PolicyID, leafHubName, err)
	}

	return nil
}

func (syncer *PoliciesDBSyncer) deletePoliciesFromDB(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	leafHubName string, policyIDsFromDB []string) error {
	for _, policyID := range policyIDsFromDB {
		if err := dbConn.DeleteAllComplianceRows(ctx, complianceTableName, policyID, leafHubName); err != nil {
			return fmt.Errorf("failed deleting compliance rows of policy '%s', leaf hub '%s' from db - %w",
				policyID, leafHubName, err)
		}
	}

	return nil
}

func (syncer *PoliciesDBSyncer) deleteSelectedComplianceRows(ctx context.Context,
	dbConn db.StatusTransportBridgeDB, leafHubName string, policyID string, clusterNames []string) error {
	for _, clusterName := range clusterNames {
		err := dbConn.DeleteComplianceRow(ctx, complianceTableName, policyID, clusterName, leafHubName)
		if err != nil {
			return fmt.Errorf("failed removing cluster '%s' of leaf hub '%s' from table status.%s - %w",
				clusterName, leafHubName, complianceTableName, err)
		}
	}

	return nil
}

// if we got the the handler function, then the bundle pre-conditions were satisfied (the generation is newer than what
// was already handled and base bundle was already handled successfully)
// we assume that 'ClustersPerPolicy' handler function handles the addition or removal of clusters rows.
// in this handler function, we handle only the existing clusters rows.
func (syncer *PoliciesDBSyncer) handleComplianceBundle(ctx context.Context, bundle bundle.Bundle,
	dbConn db.StatusTransportBridgeDB) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'ComplianceStatus' bundle", "Leaf Hub", leafHubName, "Generation",
		bundle.GetGeneration())

	policyIDsFromDB, err := dbConn.GetPolicyIDsByLeafHub(ctx, complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus, ok := object.(*statusbundle.PolicyComplianceStatus)
		if !ok {
			continue // do not handle objects other than PolicyComplianceStatus
		}

		if err := syncer.handlePolicyComplianceStatus(ctx, dbConn, leafHubName, policyComplianceStatus); err != nil {
			return fmt.Errorf("failed handling policy compliance status - %w", err)
		}
		// for policies that are found in the db but not in the bundle - all clusters are compliant (implicitly)
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, policyComplianceStatus.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}
	// update policies not in the bundle - all is compliant
	for _, policyID := range policyIDsFromDB {
		if err := dbConn.UpdatePolicyCompliance(ctx, complianceTableName, policyID, leafHubName, compliant); err != nil {
			return fmt.Errorf("failed updating policy compliance of policy '%s', leaf hub '%s' - %w", policyID,
				leafHubName, err)
		}
	}

	syncer.log.Info("finished handling 'ComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

// if we got the the handler function, then the bundle pre-conditions are satisfied.
func (syncer *PoliciesDBSyncer) handleMinimalComplianceBundle(ctx context.Context, bundle bundle.Bundle,
	dbConn db.StatusTransportBridgeDB) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	policyIDsFromDB, err := dbConn.GetPolicyIDsByLeafHub(ctx, minimalComplianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is minimal policy compliance status.
		minPolicyCompliance, ok := object.(*statusbundle.MinimalPolicyComplianceStatus)
		if !ok {
			continue // do not handle objects other than MinimalPolicyComplianceStatus.
		}

		if err := dbConn.InsertOrUpdateAggregatedPolicyCompliance(ctx, minimalComplianceTableName,
			minPolicyCompliance.PolicyID, leafHubName, syncer.getEnforcement(minPolicyCompliance.RemediationAction),
			minPolicyCompliance.AppliedClusters, minPolicyCompliance.NonCompliantClusters); err != nil {
			return fmt.Errorf("failed to update minimal compliance of policy '%s', leaf hub '%s' in db - %w",
				minPolicyCompliance.PolicyID, leafHubName, err)
		}
		// policy that is found both in db and bundle, need to remove from policiesFromDB
		// eventually we will be left with policies not in the bundle inside policyIDsFromDB and will use it to remove
		// policies that has to be deleted from the table.
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, minPolicyCompliance.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}

	// remove policies that in the db but were not sent in the bundle (leaf hub sends only living resources).
	for _, policyID := range policyIDsFromDB {
		if err := dbConn.DeleteAllComplianceRows(ctx, minimalComplianceTableName, policyID, leafHubName); err != nil {
			return fmt.Errorf("failed deleted compliance rows of policy '%s', leaf hub '%s' from db - %w",
				policyID, leafHubName, err)
		}
	}

	syncer.log.Info("finished handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

func (syncer *PoliciesDBSyncer) handlePolicyComplianceStatus(ctx context.Context, dbConn db.StatusTransportBridgeDB,
	leafHubName string, policyComplianceStatus *statusbundle.PolicyComplianceStatus) error {
	// includes both non compliant and unknown clusters
	nonCompliantClustersFromDB, err := dbConn.GetNonCompliantClustersByLeafHubAndPolicy(ctx, complianceTableName,
		leafHubName, policyComplianceStatus.PolicyID)
	if err != nil {
		return fmt.Errorf("failed getting non compliant clusters by leaf hub and policy from db - %w", err)
	}

	// update in db non compliant clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, dbConn,
		leafHubName, policyComplianceStatus.PolicyID, nonCompliant, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.NonCompliantClusters, nonCompliantClustersFromDB); err != nil {
		return fmt.Errorf("failed updating compliance rows in db - %w", err)
	}

	// update in db unknown compliance clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, dbConn,
		leafHubName, policyComplianceStatus.PolicyID, unknown, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.UnknownComplianceClusters, nonCompliantClustersFromDB); err != nil {
		return fmt.Errorf("failed updating compliance rows in db - %w", err)
	}

	// other clusters are implicitly considered as compliant
	for _, clusterName := range nonCompliantClustersFromDB { // clusters left in the non compliant from db list
		if err := dbConn.UpdateComplianceRow(ctx, complianceTableName, policyComplianceStatus.PolicyID, clusterName,
			leafHubName, compliant, policyComplianceStatus.ResourceVersion); err != nil { // change to compliant
			return fmt.Errorf("failed updating compliance rows in db - %w", err)
		}
	}

	return nil
}

func (syncer *PoliciesDBSyncer) updateSelectedComplianceRowsAndRemovedFromDBList(ctx context.Context,
	dbConn db.StatusTransportBridgeDB, leafHubName string, policyID string, compliance string, version string,
	targetClusterNames []string, clustersFromDB []string) ([]string, error) {
	for _, clusterName := range targetClusterNames { // go over the target clusters
		if err := dbConn.UpdateComplianceRow(ctx, complianceTableName, policyID, clusterName, leafHubName, compliance,
			version); err != nil {
			return clustersFromDB, fmt.Errorf("failed updating compliance row in db - %w", err)
		}

		clusterIndex, err := helpers.GetObjectIndex(clustersFromDB, clusterName)
		if err != nil {
			continue // if cluster not in the list, skip
		}

		clustersFromDB = append(clustersFromDB[:clusterIndex], clustersFromDB[clusterIndex+1:]...) // mark ad handled
	}

	return clustersFromDB, nil
}

func (syncer *PoliciesDBSyncer) getEnforcement(remediationAction policiesv1.RemediationAction) string {
	if strings.ToLower(string(remediationAction)) == inform {
		return inform
	} else if strings.ToLower(string(remediationAction)) == enforce {
		return enforce
	}

	return unknown
}
