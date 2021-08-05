package dbsyncer

import (
	"context"
	"fmt"
	"strings"
	"time"

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

// AddPoliciesTransportToDBSyncer adds policies transport to db syncer to the manager.
func AddPoliciesTransportToDBSyncer(mgr ctrl.Manager, log logr.Logger, db hohDb.StatusTransportBridgeDB,
	transport transport.Transport, managedClustersTableName string, complianceTableName string,
	minimalComplianceTableName string, clusterPerPolicyRegistration *transport.BundleRegistration,
	complianceRegistration *transport.BundleRegistration, minComplianceRegistration *transport.BundleRegistration) error {
	syncer := &PoliciesTransportToDBSyncer{
		log:                                log,
		db:                                 db,
		managedClustersTableName:           managedClustersTableName,
		complianceTableName:                complianceTableName,
		minimalComplianceTableName:         minimalComplianceTableName,
		clustersPerPolicyBundleUpdatesChan: make(chan bundle.Bundle),
		complianceBundleUpdatesChan:        make(chan bundle.Bundle),
		minimalComplianceBundleUpdatesChan: make(chan bundle.Bundle),
		bundlesGenerationLogPerLeafHub:     make(map[string]*policiesBundlesGenerationLog),
	}
	// register to bundle updates includes a predicate to filter updates when condition is false
	transport.Register(clusterPerPolicyRegistration, syncer.clustersPerPolicyBundleUpdatesChan)
	transport.Register(complianceRegistration, syncer.complianceBundleUpdatesChan)
	transport.Register(minComplianceRegistration, syncer.minimalComplianceBundleUpdatesChan)

	log.Info("initialized policies syncer")

	return mgr.Add(syncer)
}

func newPoliciesBundlesGenerationLog() *policiesBundlesGenerationLog {
	return &policiesBundlesGenerationLog{
		lastClustersPerPolicyBundleGeneration: 0,
		lastComplianceBundleGeneration:        0,
		lastMinimalComplianceBundleGeneration: 0,
	}
}

type policiesBundlesGenerationLog struct {
	lastClustersPerPolicyBundleGeneration uint64
	lastComplianceBundleGeneration        uint64
	lastMinimalComplianceBundleGeneration uint64
}

// PoliciesTransportToDBSyncer implements policies transport to db sync
type PoliciesTransportToDBSyncer struct {
	log                                logr.Logger
	db                                 hohDb.StatusTransportBridgeDB
	managedClustersTableName           string
	complianceTableName                string
	minimalComplianceTableName         string
	clustersPerPolicyBundleUpdatesChan chan bundle.Bundle
	complianceBundleUpdatesChan        chan bundle.Bundle
	minimalComplianceBundleUpdatesChan chan bundle.Bundle
	bundlesGenerationLogPerLeafHub     map[string]*policiesBundlesGenerationLog
}

// Start function starts the syncer.
func (syncer *PoliciesTransportToDBSyncer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go syncer.syncBundles(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel

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
		case clustersPerPolicyBundle := <-syncer.clustersPerPolicyBundleUpdatesChan:
			leafHubName := clustersPerPolicyBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)
			go func() {
				if err := helpers.HandleBundle(ctx, clustersPerPolicyBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastClustersPerPolicyBundleGeneration,
					syncer.handleClustersPerPolicyBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					syncer.handleRetry(clustersPerPolicyBundle, syncer.clustersPerPolicyBundleUpdatesChan)
				}
			}()

		case complianceBundle := <-syncer.complianceBundleUpdatesChan:
			leafHubName := complianceBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)
			go func() {
				if err := helpers.HandleBundle(ctx, complianceBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastComplianceBundleGeneration,
					syncer.handleComplianceBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					syncer.handleRetry(complianceBundle, syncer.complianceBundleUpdatesChan)
				}
			}()

		case minimalComplianceBundle := <-syncer.minimalComplianceBundleUpdatesChan:
			leafHubName := minimalComplianceBundle.GetLeafHubName()
			syncer.createBundleGenerationLogIfNotExist(leafHubName)
			go func() {
				if err := helpers.HandleBundle(ctx, minimalComplianceBundle,
					&syncer.bundlesGenerationLogPerLeafHub[leafHubName].lastMinimalComplianceBundleGeneration,
					syncer.handleMinimalComplianceBundle); err != nil {
					syncer.log.Error(err, "failed to handle bundle")
					syncer.handleRetry(minimalComplianceBundle, syncer.minimalComplianceBundleUpdatesChan)
				}
			}()
		}
	}
}

func (syncer *PoliciesTransportToDBSyncer) handleRetry(bundle bundle.Bundle, bundleChan chan bundle.Bundle) {
	time.Sleep(time.Second) // TODO reschedule, should use exponential back off
	bundleChan <- bundle
}

// on the first time a new leaf hub connect, it needs to create bundle generation log, to manage the generation of
// the bundles we get from that specific leaf hub
func (syncer *PoliciesTransportToDBSyncer) createBundleGenerationLogIfNotExist(leafHubName string) {
	if _, found := syncer.bundlesGenerationLogPerLeafHub[leafHubName]; !found {
		syncer.bundlesGenerationLogPerLeafHub[leafHubName] = newPoliciesBundlesGenerationLog()
	}
}

// if we got inside the handler function, then the bundle generation is newer than what we have in memory
// handling bundle clusters per policy only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function
func (syncer *PoliciesTransportToDBSyncer) handleClustersPerPolicyBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub policies from db - %w", err)
	}

	for _, object := range bundle.GetObjects() { // every object is clusters list + enforcement per policy
		clustersPerPolicy := object.(*statusbundle.ClustersPerPolicy)
		clustersFromDB, err := syncer.db.GetComplianceClustersByLeafHubAndPolicy(ctx, syncer.complianceTableName,
			leafHubName, clustersPerPolicy.PolicyID)
		if err != nil {
			return fmt.Errorf("failed to get clusters from db - %w", err)
		}

		for _, clusterName := range clustersPerPolicy.Clusters { // go over the received bundle clusters per policy
			if !syncer.db.ManagedClusterExists(ctx, syncer.managedClustersTableName, leafHubName, clusterName) {
				return fmt.Errorf(`cluster %s doesn't exist, reschduling 'ClustersPerPolicy' bundle from leaf 
					"hub '%s', generation %d`, clusterName, leafHubName, bundle.GetGeneration())
			}

			clusterIndex, err := helpers.GetObjectIndex(clustersFromDB, clusterName)
			if err != nil { // cluster not found in the compliance table (and exists in managed clusters status table)
				if err = syncer.db.InsertPolicyCompliance(ctx, syncer.complianceTableName, clustersPerPolicy.PolicyID,
					clusterName, leafHubName, errorNone, unknown, syncer.getEnforcement(
						clustersPerPolicy.RemediationAction), clustersPerPolicy.ResourceVersion); err != nil {
					return fmt.Errorf("failed to insert cluster compliance to DB - %w", err)
				}
				continue
			}
			// if we got here, the compliance row exists both in db and in the received bundle.
			// remove from complianceRowsFromDB since we don't update the row in clusters per policy bundle, only insert
			// new compliance rows or delete non relevant rows
			clustersFromDB = append(clustersFromDB[:clusterIndex], clustersFromDB[clusterIndex+1:]...)
		}
		// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources)
		err = syncer.deleteSelectedComplianceRows(ctx, leafHubName, clustersPerPolicy.PolicyID, clustersFromDB)
		if err != nil {
			return fmt.Errorf("failed deleting compliance rows from db - %w", err)
		}
		// update enforcement and version of all rows with leafHub and policyId
		if err = syncer.db.UpdateEnforcementAndResourceVersion(ctx, syncer.complianceTableName,
			clustersPerPolicy.PolicyID, leafHubName, syncer.getEnforcement(clustersPerPolicy.RemediationAction),
			clustersPerPolicy.ResourceVersion); err != nil {
			return fmt.Errorf("failed updating enforcement and resource version in db - %w", err)
		}
		// keep this policy in db, should remove from db policies that were not sent in the bundle
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, clustersPerPolicy.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...) // policy exists
		}
	}
	// remove all policies not in the bundle
	for _, policyID := range policyIDsFromDB {
		if err := syncer.db.DeleteAllComplianceRows(ctx, syncer.complianceTableName, policyID, leafHubName); err != nil {
			return fmt.Errorf("failed deleted compliance rows from db - %w", err)
		}
	}
	syncer.log.Info("finished handling 'ClustersPerPolicy' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())
	return nil
}

func (syncer *PoliciesTransportToDBSyncer) deleteSelectedComplianceRows(ctx context.Context, leafHubName string,
	policyID string, clusterNames []string) error {
	for _, clusterName := range clusterNames {
		err := syncer.db.DeleteComplianceRow(ctx, syncer.complianceTableName, policyID, clusterName, leafHubName)
		if err != nil {
			return fmt.Errorf("failed to remove object %s from table status.%s", clusterName,
				syncer.complianceTableName)
		}
	}

	return nil
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory
// we assume that 'ClustersPerPolicy' handler function handles the addition or removal of clusters rows.
// in this handler function, we handle only the existing clusters rows.
func (syncer *PoliciesTransportToDBSyncer) handleComplianceBundle(ctx context.Context, bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'ComplianceStatus' bundle", "Leaf Hub", leafHubName, "Generation",
		bundle.GetGeneration())

	if shouldProcessBundle, err := syncer.checkComplianceBundlePreConditions(bundle); !shouldProcessBundle {
		return err // in case the bundle has to be rescheduled returns an error, otherwise returns nil (bundle dropped)
	}

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.complianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub policies from db - %w", err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus := object.(*statusbundle.PolicyComplianceStatus)
		if err := syncer.handlePolicyComplianceStatus(ctx, leafHubName, policyComplianceStatus); err != nil {
			return fmt.Errorf("failed handling policy compliance status - %w", err)
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
			return fmt.Errorf("failed updating policy compliance - %w", err)
		}
	}

	syncer.log.Info("finished handling 'ComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())
	return nil
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory
func (syncer *PoliciesTransportToDBSyncer) handleMinimalComplianceBundle(ctx context.Context,
	bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	syncer.log.Info("start handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())

	policyIDsFromDB, err := syncer.db.GetPolicyIDsByLeafHub(ctx, syncer.minimalComplianceTableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub policies from db - %w", err)
	}

	for _, object := range bundle.GetObjects() { // every object in bundle is minimal policy compliance status
		minPolicyCompliance := object.(*statusbundle.MinimalPolicyComplianceStatus)
		if err := syncer.db.InsertOrUpdateAggregatedPolicyCompliance(ctx, syncer.minimalComplianceTableName,
			minPolicyCompliance.PolicyID, leafHubName, syncer.getEnforcement(minPolicyCompliance.RemediationAction),
			minPolicyCompliance.AppliedClusters, minPolicyCompliance.NonCompliantClusters); err != nil {
			return fmt.Errorf("failed to update minimal policy compliance status in db - %w", err)
		}
		// policy that is found both in db and bundle, need to remove from policiesFromDB
		// eventually we will be left with policies not in the bundle inside policyIDsFromDB and will use it to remove
		// policies that has to be deleted from the table
		if policyIndex, err := helpers.GetObjectIndex(policyIDsFromDB, minPolicyCompliance.PolicyID); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}

	// remove policies that in the db but were not sent in the bundle (leaf hub sends only living resources)
	for _, policyID := range policyIDsFromDB {
		if err := syncer.db.DeleteAllComplianceRows(ctx, syncer.minimalComplianceTableName, policyID,
			leafHubName); err != nil {
			return fmt.Errorf("failed deleted compliance rows from db - %w", err)
		}
	}

	syncer.log.Info("finished handling 'MinimalComplianceStatus' bundle", "Leaf Hub", leafHubName,
		"Generation", bundle.GetGeneration())
	return nil
}

// return bool,err
// bool - if this bundle should be processed or not
// error - in case of an error the bundle will be rescheduled
func (syncer *PoliciesTransportToDBSyncer) checkComplianceBundlePreConditions(receivedBundle bundle.Bundle) (bool,
	error) {
	leafHubName := receivedBundle.GetLeafHubName()
	complianceBundle := receivedBundle.(*bundle.ComplianceStatusBundle)
	// if the base wasn't handled yet
	if complianceBundle.BaseBundleGeneration > syncer.bundlesGenerationLogPerLeafHub[leafHubName].
		lastClustersPerPolicyBundleGeneration {
		return false, fmt.Errorf(`waiting for base bundle %d to be handled, reschduling 'ComplianceStatus' 
			"bundle from leaf hub '%s', generation %d`, complianceBundle.BaseBundleGeneration, leafHubName,
			receivedBundle.GetGeneration())
	}

	return true, nil
}

func (syncer *PoliciesTransportToDBSyncer) handlePolicyComplianceStatus(ctx context.Context, leafHubName string,
	policyComplianceStatus *statusbundle.PolicyComplianceStatus) error {
	// includes both non compliant and unknown clusters
	nonCompliantClustersFromDB, err := syncer.db.GetNonCompliantClustersByLeafHubAndPolicy(ctx,
		syncer.complianceTableName, leafHubName, policyComplianceStatus.PolicyID)
	if err != nil {
		return err
	}

	// update in db non compliant clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, leafHubName,
		policyComplianceStatus.PolicyID, nonCompliant, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.NonCompliantClusters, nonCompliantClustersFromDB); err != nil {
		return err
	}

	// update in db unknown compliance clusters
	if nonCompliantClustersFromDB, err = syncer.updateSelectedComplianceRowsAndRemovedFromDBList(ctx, leafHubName,
		policyComplianceStatus.PolicyID, unknown, policyComplianceStatus.ResourceVersion,
		policyComplianceStatus.UnknownComplianceClusters, nonCompliantClustersFromDB); err != nil {
		return err
	}

	// other clusters are implicitly considered as compliant
	for _, clusterName := range nonCompliantClustersFromDB { // clusters left in the non compliant from db list
		if err := syncer.db.UpdateComplianceRow(ctx, syncer.complianceTableName, policyComplianceStatus.PolicyID, clusterName,
			leafHubName, compliant, policyComplianceStatus.ResourceVersion); err != nil { // change to compliant
			return err
		}
	}

	return nil
}

func (syncer *PoliciesTransportToDBSyncer) updateSelectedComplianceRowsAndRemovedFromDBList(ctx context.Context,
	leafHubName string, policyID string, compliance string, version string, targetClusterNames []string,
	clustersFromDB []string) ([]string, error) {
	for _, clusterName := range targetClusterNames { // go over the target clusters
		if err := syncer.db.UpdateComplianceRow(ctx, syncer.complianceTableName, policyID, clusterName, leafHubName,
			compliance, version); err != nil {
			return clustersFromDB, err
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
