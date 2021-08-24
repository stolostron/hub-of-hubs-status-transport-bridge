package dbsyncer

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	v1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/helpers"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	errorNone = "none"

	nonCompliant = "non_compliant"
	compliant    = "compliant"
	unknown      = "unknown"

	inform  = "inform"
	enforce = "enforce"
)

var (
	errBundleWrongType  = errors.New("wrong type of bundle")
	errRescheduleBundle = errors.New("rescheduling bundle")
)

// AddPoliciesTransportToDBSyncer adds policies transport to db syncer to the manager.
func AddPoliciesTransportToDBSyncer(mgr ctrl.Manager, log logr.Logger, db hohDb.PoliciesStatusDB,
	transport transport.Transport, managedClustersTableName string, complianceTableName string,
	minimalComplianceTableName string, clusterPerPolicyRegistration *transport.BundleRegistration,
	completeComplianceRegistration *transport.BundleRegistration,
	deltaComplianceRegistration *transport.BundleRegistration,
	minComplianceRegistration *transport.BundleRegistration) error {
	syncer := &PoliciesTransportToDBSyncer{
		log:                                 log,
		db:                                  db,
		transport:                           transport,
		managedClustersTableName:            managedClustersTableName,
		complianceTableName:                 complianceTableName,
		minimalComplianceTableName:          minimalComplianceTableName,
		clustersPerPolicyBundleUpdatesChan:  make(chan bundle.Bundle),
		completeComplianceBundleUpdatesChan: make(chan bundle.Bundle),
		deltaComplianceBundleUpdatesChan:    make(chan bundle.Bundle),
		minimalComplianceBundleUpdatesChan:  make(chan bundle.Bundle),
		bundlesGenerationLogPerLeafHub:      make(map[string]*policiesBundlesGenerationLog),
	}
	// register to bundle updates includes a predicate to filter updates when condition is false.
	transport.Register(clusterPerPolicyRegistration, syncer.clustersPerPolicyBundleUpdatesChan)
	transport.Register(completeComplianceRegistration, syncer.completeComplianceBundleUpdatesChan)
	transport.Register(deltaComplianceRegistration, syncer.deltaComplianceBundleUpdatesChan)
	transport.Register(minComplianceRegistration, syncer.minimalComplianceBundleUpdatesChan)

	log.Info("initialized policies syncer")

	if err := mgr.Add(syncer); err != nil {
		return fmt.Errorf("failed to add transport to db syncer to manager - %w", err)
	}

	return nil
}

func newPoliciesBundlesGenerationLog() *policiesBundlesGenerationLog {
	return &policiesBundlesGenerationLog{
		lastClustersPerPolicyBundleGeneration:  0,
		lastCompleteComplianceBundleGeneration: 0,
		lastDeltaComplianceBundleGeneration:    0,
		lastMinimalComplianceBundleGeneration:  0,
	}
}

type policiesBundlesGenerationLog struct {
	lastClustersPerPolicyBundleGeneration  uint64
	lastCompleteComplianceBundleGeneration uint64
	lastDeltaComplianceBundleGeneration    uint64
	lastMinimalComplianceBundleGeneration  uint64
}

// PoliciesTransportToDBSyncer implements policies transport to db sync.
type PoliciesTransportToDBSyncer struct {
	log                                 logr.Logger
	db                                  hohDb.PoliciesStatusDB
	transport                           transport.Transport
	managedClustersTableName            string
	complianceTableName                 string
	minimalComplianceTableName          string
	clustersPerPolicyBundleUpdatesChan  chan bundle.Bundle
	completeComplianceBundleUpdatesChan chan bundle.Bundle
	deltaComplianceBundleUpdatesChan    chan bundle.Bundle
	minimalComplianceBundleUpdatesChan  chan bundle.Bundle
	bundlesGenerationLogPerLeafHub      map[string]*policiesBundlesGenerationLog
}

// Start function starts the syncer.
func (syncer *PoliciesTransportToDBSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.syncBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel.

		syncer.log.Info("stopped policies transport to db syncer")
		cancelContext()

		return nil
	}
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (syncer *PoliciesTransportToDBSyncer) syncBundles(ctx context.Context) {
	for {
		select { // wait for incoming bundles to handle
		case <-ctx.Done():
			return

		case clustersPerPolicyBundle := <-syncer.clustersPerPolicyBundleUpdatesChan:
			leafHubName := clustersPerPolicyBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)

			go func() {
				if err := helpers.HandleBundle(ctx, syncer.transport, clustersPerPolicyBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastClustersPerPolicyBundleGeneration,
					syncer.handleClustersPerPolicyBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					helpers.HandleRetry(clustersPerPolicyBundle, syncer.clustersPerPolicyBundleUpdatesChan)
				}
			}()

		case completeComplianceBundle := <-syncer.completeComplianceBundleUpdatesChan:
			leafHubName := completeComplianceBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)

			go func() {
				if err := helpers.HandleBundle(ctx, syncer.transport, completeComplianceBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastCompleteComplianceBundleGeneration,
					syncer.handleCompleteComplianceBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					helpers.HandleRetry(completeComplianceBundle, syncer.completeComplianceBundleUpdatesChan)
				}
			}()

		case deltaComplianceBundle := <-syncer.deltaComplianceBundleUpdatesChan:
			leafHubName := deltaComplianceBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)

			go func() {
				if err := helpers.HandleBundle(ctx, syncer.transport, deltaComplianceBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastCompleteComplianceBundleGeneration,
					syncer.handleDeltaComplianceBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					helpers.HandleRetry(deltaComplianceBundle, syncer.deltaComplianceBundleUpdatesChan)
				}
			}()

		case minimalComplianceBundle := <-syncer.minimalComplianceBundleUpdatesChan:
			leafHubName := minimalComplianceBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)

			go func() {
				if err := helpers.HandleBundle(ctx, syncer.transport, minimalComplianceBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastMinimalComplianceBundleGeneration,
					syncer.handleMinimalComplianceBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					helpers.HandleRetry(minimalComplianceBundle, syncer.minimalComplianceBundleUpdatesChan)
				}
			}()
		}
	}
}

// on the first time a new leaf hub connect, it needs to create bundle generation log, to manage the generation of
// the bundles we get from that specific leaf hub.
func (syncer *PoliciesTransportToDBSyncer) createBundleGenerationLogIfNotExist(leafHubName string) {
	if _, found := syncer.bundlesGenerationLogPerLeafHub[leafHubName]; !found {
		syncer.bundlesGenerationLogPerLeafHub[leafHubName] = newPoliciesBundlesGenerationLog()
	}
}

// if we got inside the handler function, then the bundle generation is newer than what we have in memory
// handling bundle clusters per policy only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function.
func (syncer *PoliciesTransportToDBSyncer) handleClustersPerPolicyBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	bundleGeneration := bundle.GetGeneration()

	syncer.log.Info("start handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGeneration)

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object is clusters list + enforcement per policy
		clustersPerPolicy, ok := object.(*statusbundle.ClustersPerPolicy)
		if !ok {
			continue // do not handle objects other than ClustersPerPolicy
		}

		if err := syncer.handleClusterPerPolicy(ctx, leafHubName, bundleGeneration, clustersPerPolicy); err != nil {
			return fmt.Errorf("failed handling clusters per policy bundle - %w", err)
		}

		// keep this policy in db, should remove from db policies that were not sent in the bundle
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, clustersPerPolicy.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...) // policy exists
		}
	}
	// remove policies that were not sent in the bundle
	if err := syncer.deletePoliciesFromDB(ctx, leafHubName, policyIDsFromDB); err != nil {
		return fmt.Errorf("failed deleting policies from db - %w", err)
	}

	syncer.log.Info("finished handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundleGeneration)

	return nil
}

func (syncer *PoliciesTransportToDBSyncer) handleClusterPerPolicy(ctx context.Context, leafHubName string,
	bundleGeneration uint64, clustersPerPolicy *statusbundle.ClustersPerPolicy) error {
	clustersFromDB, err := syncer.db.GetComplianceClustersByLeafHubAndPolicy(ctx, syncer.complianceTableName,
		leafHubName, clustersPerPolicy.PolicyID)
	if err != nil {
		return fmt.Errorf("failed to get clusters by leaf hub and policy from db - %w", err)
	}

	for _, clusterName := range clustersPerPolicy.Clusters { // go over the clusters per policy
		if !syncer.db.ManagedClusterExists(ctx, syncer.managedClustersTableName, leafHubName, clusterName) {
			return fmt.Errorf(`%w 'ClustersPerPolicy' from leaf hub '%s', generation %d - cluster '%s' 
					doesn't exist`, errRescheduleBundle, leafHubName, bundleGeneration, clusterName)
		}

		clusterIndex, err := helpers.GetObjectIndex(clustersFromDB, clusterName)
		if err != nil { // cluster not found in the compliance table (and exists in managed clusters status table)
			if err = syncer.db.InsertPolicyCompliance(ctx, syncer.complianceTableName, clustersPerPolicy.PolicyID,
				clusterName, leafHubName, errorNone, unknown, syncer.getEnforcement(
					clustersPerPolicy.RemediationAction), clustersPerPolicy.ResourceVersion); err != nil {
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
	err = syncer.deleteSelectedComplianceRows(ctx, leafHubName, clustersPerPolicy.PolicyID, clustersFromDB)
	if err != nil {
		return fmt.Errorf("failed deleting compliance rows of policy '%s', leaf hub '%s' from db - %w",
			clustersPerPolicy.PolicyID, leafHubName, err)
	}
	// update enforcement and version of all rows with leafHub and policyId
	if err = syncer.db.UpdateEnforcementAndResourceVersion(ctx, syncer.complianceTableName,
		clustersPerPolicy.PolicyID, leafHubName, syncer.getEnforcement(clustersPerPolicy.RemediationAction),
		clustersPerPolicy.ResourceVersion); err != nil {
		return fmt.Errorf(`failed updating enforcement and resource version of policy '%s', leaf hub '%s' 
					in db - %w`, clustersPerPolicy.PolicyID, leafHubName, err)
	}

	return nil
}

func (syncer *PoliciesTransportToDBSyncer) deletePoliciesFromDB(ctx context.Context, leafHubName string,
	policyIDsFromDB []string) error {
	for _, policyID := range policyIDsFromDB {
		if err := syncer.db.DeleteAllComplianceRows(ctx, syncer.complianceTableName,
			policyID, leafHubName); err != nil {
			return fmt.Errorf("failed deleting compliance rows of policy '%s', leaf hub '%s' from db - %w",
				policyID, leafHubName, err)
		}
	}

	return nil
}

func (syncer *PoliciesTransportToDBSyncer) deleteSelectedComplianceRows(ctx context.Context, leafHubName string,
	policyID string, clusterNames []string) error {
	for _, clusterName := range clusterNames {
		err := syncer.db.DeleteComplianceRow(ctx, syncer.complianceTableName, policyID, clusterName, leafHubName)
		if err != nil {
			return fmt.Errorf("failed removing cluster '%s' of leaf hub '%s' from table status.%s - %w",
				clusterName, leafHubName, syncer.complianceTableName, err)
		}
	}

	return nil
}

// if we got to the handler function, then the bundle generation is newer than what we have in memory
// we assume that 'ClustersPerPolicy' handler function handles the addition or removal of clusters rows.
// in this handler function, we handle only the existing clusters rows.
func (syncer *PoliciesTransportToDBSyncer) handleCompleteComplianceBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'CompleteComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	if shouldProcessBundle, err := syncer.checkCompleteComplianceBundlePreConditions(bundle); !shouldProcessBundle {
		return err // in case the bundle has to be rescheduled returns an error, otherwise returns nil (bundle dropped)
	}

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus, ok := object.(*statusbundle.PolicyCompleteComplianceStatus)
		if !ok {
			continue // do not handle objects other than PolicyCompleteComplianceStatus
		}

		if err := syncer.handleCompletePolicyComplianceStatus(ctx, leafHubName, policyComplianceStatus); err != nil {
			return fmt.Errorf("failed handling policy complete-compliance status - %w", err)
		}
		// for policies that are found in the db but not in the bundle - all clusters are compliant (implicitly)
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, policyComplianceStatus.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}
	// update policies not in the bundle - all is compliant
	for _, policyID := range policyIDsFromDB {
		err := syncer.db.UpdatePolicyCompliance(ctx, syncer.complianceTableName, policyID, leafHubName, compliant)
		if err != nil {
			return fmt.Errorf("failed updating policy compliance of policy '%s', leaf hub '%s' - %w", policyID,
				leafHubName, err)
		}
	}

	syncer.log.Info("finished handling 'CompleteComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

// if we got to the handler function, then the bundle generation is newer than what we have in memory
// we assume that 'ClustersPerPolicy' handler function handles the addition or removal of clusters rows.
// in this handler function, we apply the bundle content as-is.
func (syncer *PoliciesTransportToDBSyncer) handleDeltaComplianceBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'DeltaComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	if shouldProcessBundle, err := syncer.checkDeltaComplianceBundlePreConditions(bundle); !shouldProcessBundle {
		return err // in case the bundle has to be rescheduled returns an error, otherwise returns nil (bundle dropped)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is policy delta compliance status
		policyComplianceStatus, ok := object.(*statusbundle.PolicyDeltaComplianceStatus)
		if !ok {
			continue // do not handle objects other than PolicyDeltaComplianceStatus
		}

		err := syncer.handleDeltaPolicyComplianceStatus(ctx, leafHubName, policyComplianceStatus.PolicyID, compliant,
			policyComplianceStatus.ResourceVersion, policyComplianceStatus.CompliantClusters)
		if err != nil {
			return err
		}

		err = syncer.handleDeltaPolicyComplianceStatus(ctx, leafHubName, policyComplianceStatus.PolicyID, nonCompliant,
			policyComplianceStatus.ResourceVersion, policyComplianceStatus.NonCompliantClusters)
		if err != nil {
			return err
		}

		err = syncer.handleDeltaPolicyComplianceStatus(ctx, leafHubName, policyComplianceStatus.PolicyID, unknown,
			policyComplianceStatus.ResourceVersion, policyComplianceStatus.UnknownComplianceClusters)
		if err != nil {
			return err
		}
	}

	syncer.log.Info("finished handling 'DeltaComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory.
func (syncer *PoliciesTransportToDBSyncer) handleMinimalComplianceBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.minimalComplianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s' policies from db - %w", leafHubName, err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is minimal policy compliance status.
		minPolicyCompliance, ok := object.(*statusbundle.MinimalPolicyComplianceStatus)
		if !ok {
			continue // do not handle objects other than MinimalPolicyComplianceStatus.
		}

		if err := syncer.db.InsertOrUpdateAggregatedPolicyCompliance(ctx, syncer.minimalComplianceTableName,
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
		if err := syncer.db.DeleteAllComplianceRows(ctx, syncer.minimalComplianceTableName, policyID,
			leafHubName); err != nil {
			return fmt.Errorf("failed deleted compliance rows of policy '%s', leaf hub '%s' from db - %w",
				policyID, leafHubName, err)
		}
	}

	syncer.log.Info("finished handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	return nil
}

// return bool,err
// bool - if this bundle should be processed or not
// error - in case of an error the bundle will be rescheduled.
func (syncer *PoliciesTransportToDBSyncer) checkCompleteComplianceBundlePreConditions(receivedBundle bundle.Bundle) (
	bool, error) {
	leafHubName := receivedBundle.GetLeafHubName()

	complianceBundle, ok := receivedBundle.(*bundle.CompleteComplianceStatusBundle)
	if !ok {
		return false, errBundleWrongType // do not handle objects other than CompleteComplianceStatusBundle
	}

	// if the base wasn't handled yet
	if complianceBundle.BaseBundleGeneration > syncer.bundlesGenerationLogPerLeafHub[leafHubName].
		lastClustersPerPolicyBundleGeneration {
		return false, fmt.Errorf(`%w 'CompleteComplianceStatus' from leaf hub '%s', generation %d - waiting for base 
			bundle %d to be handled`, errRescheduleBundle, leafHubName, receivedBundle.GetGeneration(),
			complianceBundle.BaseBundleGeneration)
	}

	return true, nil
}

// return bool,err
// bool - if this bundle should be processed or not
// error - in case of an error the bundle will be rescheduled.
func (syncer *PoliciesTransportToDBSyncer) checkDeltaComplianceBundlePreConditions(receivedBundle bundle.Bundle) (bool,
	error) {
	leafHubName := receivedBundle.GetLeafHubName()

	complianceBundle, ok := receivedBundle.(*bundle.DeltaComplianceStatusBundle)
	if !ok {
		return false, errBundleWrongType // do not handle objects other than DeltaComplianceStatusBundle
	}

	// if the base wasn't handled yet
	if complianceBundle.BaseBundleGeneration > syncer.bundlesGenerationLogPerLeafHub[leafHubName].
		lastCompleteComplianceBundleGeneration {
		return false, fmt.Errorf(`%w 'DeltaComplianceStatus' from leaf hub '%s', generation %d - waiting for base 
			bundle %d to be handled`, errRescheduleBundle, leafHubName, receivedBundle.GetGeneration(),
			complianceBundle.BaseBundleGeneration)
	}

	return true, nil
}

func (syncer *PoliciesTransportToDBSyncer) handleCompletePolicyComplianceStatus(ctx context.Context, leafHubName string,
	policyComplianceStatus *statusbundle.PolicyCompleteComplianceStatus) error {
	// includes both non compliant and unknown clusters
	nonCompliantClustersFromDB, err := syncer.db.GetNonCompliantClustersByLeafHubAndPolicy(ctx,
		syncer.complianceTableName, leafHubName, policyComplianceStatus.PolicyID)
	if err != nil {
		return fmt.Errorf("failed getting non compliant clusters by leaf hub and policy from db - %w", err)
	}

	// update in db non compliant clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, leafHubName,
		policyComplianceStatus.PolicyID, nonCompliant, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.NonCompliantClusters, nonCompliantClustersFromDB); err != nil {
		return fmt.Errorf("failed updating compliance rows in db - %w", err)
	}

	// update in db unknown compliance clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, leafHubName,
		policyComplianceStatus.PolicyID, unknown, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.UnknownComplianceClusters, nonCompliantClustersFromDB); err != nil {
		return fmt.Errorf("failed updating compliance rows in db - %w", err)
	}

	// other clusters are implicitly considered as compliant
	for _, clusterName := range nonCompliantClustersFromDB { // clusters left in the non compliant from db list
		if err := syncer.db.UpdateComplianceRow(ctx, syncer.complianceTableName, policyComplianceStatus.PolicyID, clusterName,
			leafHubName, compliant, policyComplianceStatus.ResourceVersion); err != nil { // change to compliant
			return fmt.Errorf("failed updating compliance rows in db - %w", err)
		}
	}

	return nil
}

// handleDeltaPolicyComplianceStatus updates db with leaf hub's given clusters with the given status as-is.
func (syncer *PoliciesTransportToDBSyncer) handleDeltaPolicyComplianceStatus(ctx context.Context,
	leafHubName string, policyID string, compliance string, version string, targetClusterNames []string) error {
	if len(targetClusterNames) == 0 {
		return nil
	}

	if err := syncer.db.UpdateComplianceSet(ctx, syncer.complianceTableName, policyID, targetClusterNames,
		leafHubName, compliance, version); err != nil {
		return fmt.Errorf("failed updating compliance rows in db - %w", err)
	}

	return nil
}

func (syncer *PoliciesTransportToDBSyncer) updateSelectedComplianceRowsAndRemovedFromDBList(ctx context.Context,
	leafHubName string, policyID string, compliance string, version string, targetClusterNames []string,
	clustersFromDB []string) ([]string, error) {
	for _, clusterName := range targetClusterNames { // go over the target clusters
		if err := syncer.db.UpdateComplianceRow(ctx, syncer.complianceTableName, policyID, clusterName, leafHubName,
			compliance, version); err != nil {
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

func (syncer *PoliciesTransportToDBSyncer) getEnforcement(remediationAction v1.RemediationAction) string {
	if strings.ToLower(string(remediationAction)) == inform {
		return inform
	} else if strings.ToLower(string(remediationAction)) == enforce {
		return enforce
	}

	return unknown
}
