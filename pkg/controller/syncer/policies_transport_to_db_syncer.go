package syncer

import (
	"fmt"
	v1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"
	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	hohDb "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"log"
	"strings"
	"time"
)

const (
	errorNone = "none"

	nonCompliant = "non_compliant"
	compliant    = "compliant"
	unknown      = "unknown"

	inform  = "inform"
	enforce = "enforce"
)

func NewPoliciesTransportToDBSyncer(db hohDb.StatusTransportBridgeDB, transport transport.Transport,
	managedClustersTableName string, complianceTableName string, clusterPerPolicyRegistration *BundleRegistration,
	complianceRegistration *BundleRegistration, stopChan chan struct{}) *PoliciesTransportToDBSyncer {
	syncer := &PoliciesTransportToDBSyncer{
		db:                                 db,
		managedClustersTableName:           managedClustersTableName,
		complianceTableName:                complianceTableName,
		clustersPerPolicyBundleUpdatesChan: make(chan bundle.Bundle),
		complianceBundleUpdatesChan:        make(chan bundle.Bundle),
		bundlesGenerationLogPerLeafHub:     make(map[string]*policiesBundlesGenerationLog),
		stopChan:                           stopChan,
	}
	registerToBundleUpdates(transport, clusterPerPolicyRegistration, syncer.clustersPerPolicyBundleUpdatesChan)
	registerToBundleUpdates(transport, complianceRegistration, syncer.complianceBundleUpdatesChan)
	log.Println(fmt.Sprintf("initialized syncer for table status.%s", complianceTableName))
	return syncer
}

func newPoliciesBundlesGenerationLog() *policiesBundlesGenerationLog {
	return &policiesBundlesGenerationLog{
		lastClustersPerPolicyBundleGeneration: 0,
		lastComplianceBundleGeneration:        0,
	}
}

type policiesBundlesGenerationLog struct {
	lastClustersPerPolicyBundleGeneration uint64
	lastComplianceBundleGeneration        uint64
}

type PoliciesTransportToDBSyncer struct {
	db                                 hohDb.StatusTransportBridgeDB
	managedClustersTableName           string
	complianceTableName                string
	clustersPerPolicyBundleUpdatesChan chan bundle.Bundle
	complianceBundleUpdatesChan        chan bundle.Bundle
	bundlesGenerationLogPerLeafHub     map[string]*policiesBundlesGenerationLog
	stopChan                           chan struct{}
}

func (s *PoliciesTransportToDBSyncer) StartSync() {
	go s.syncBundles()
}

// need to do "diff" between objects received in the bundle and the objects in db.
// leaf hub sends only the current existing objects, and status transport bridge should understand implicitly which
// objects were deleted.
// therefore, whatever is in the db and cannot be found in the bundle has to be deleted from the db.
// for the objects that appear in both, need to check if something has changed using resourceVersion field comparison
// and if the object was changed, update the db with the current object.
func (s *PoliciesTransportToDBSyncer) syncBundles() {
	for {
		select { // wait for incoming bundles to handle
		case <-s.stopChan:
			return
		case clustersPerPolicyBundle := <-s.clustersPerPolicyBundleUpdatesChan:
			leafHubName := clustersPerPolicyBundle.GetLeafHubName()
			s.createBundleGenerationLogIfNotExist(leafHubName)
			go func() {
				if err := handleBundle(clustersPerPolicyBundle,
					&s.bundlesGenerationLogPerLeafHub[leafHubName].lastClustersPerPolicyBundleGeneration,
					s.handleClustersPerPolicyBundle); err != nil {
					log.Println(err) // TODO reschedule, should use exponential back off
					time.Sleep(time.Second)
					s.clustersPerPolicyBundleUpdatesChan <- clustersPerPolicyBundle
				}
			}()
		case complianceBundle := <-s.complianceBundleUpdatesChan:
			leafHubName := complianceBundle.GetLeafHubName()
			s.createBundleGenerationLogIfNotExist(leafHubName)
			go func() {
				if err := handleBundle(complianceBundle,
					&s.bundlesGenerationLogPerLeafHub[leafHubName].lastComplianceBundleGeneration,
					s.handleComplianceBundle); err != nil {
					log.Println(err) // TODO reschedule, should use exponential back off
					time.Sleep(time.Second)
					s.complianceBundleUpdatesChan <- complianceBundle
				}
			}()
		}
	}
}

// on the first time a new leaf hub connect, it needs to create bundle generation log, to manage the generation of
// the bundles we get from that specific leaf hub
func (s *PoliciesTransportToDBSyncer) createBundleGenerationLogIfNotExist(leafHubName string) {
	if _, found := s.bundlesGenerationLogPerLeafHub[leafHubName]; !found {
		s.bundlesGenerationLogPerLeafHub[leafHubName] = newPoliciesBundlesGenerationLog()
	}
}

// if we got inside the handler function, then the bundle generation is newer than what we have in memory
// handling bundle clusters per policy only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function
func (s *PoliciesTransportToDBSyncer) handleClustersPerPolicyBundle(receivedBundle bundle.Bundle) error {
	leafHubName := receivedBundle.GetLeafHubName()
	log.Println(fmt.Sprintf("start handling 'ClustersPerPolicy' bundle from leaf hub '%s', generation %d",
		leafHubName, receivedBundle.GetGeneration()))
	policyIDsFromDB, err := s.db.GetPolicyIDsByLeafHub(s.complianceTableName, leafHubName)
	if err != nil {
		return err
	}
	for _, object := range receivedBundle.GetObjects() { // every object is clusters list + enforcement per policy
		clustersPerPolicy := object.(*statusbundle.ClustersPerPolicy)
		clustersFromDB, err := s.db.GetComplianceClustersByLeafHubAndPolicy(s.complianceTableName, leafHubName,
			clustersPerPolicy.PolicyId)
		if err != nil {
			return err
		}
		for _, clusterName := range clustersPerPolicy.Clusters { // go over the received bundle clusters per policy
			if !s.db.ManagedClusterExists(s.managedClustersTableName, leafHubName, clusterName) {
				return fmt.Errorf("cluster %s doesn't exist, reschduling 'ClustersPerPolicy' bundle from leaf "+
					"hub '%s', generation %d", clusterName, leafHubName, receivedBundle.GetGeneration())
			}
			clusterIndex, err := getObjectIndex(clustersFromDB, clusterName)
			if err != nil { // cluster not found in the compliance table (and exists in managed clusters status table)
				if err = s.db.InsertPolicyCompliance(s.complianceTableName, clustersPerPolicy.PolicyId, clusterName,
					leafHubName, errorNone, unknown, s.getEnforcement(clustersPerPolicy.RemediationAction),
					clustersPerPolicy.ResourceVersion); err != nil {
					log.Println(err) // failed to insert cluster compliance to DB // TODO schedule retry
				}
				continue
			}
			// if we got here, the compliance row exists both in db and in the received bundle.
			// remove from complianceRowsFromDB since we don't update the row in clusters per policy bundle, only insert
			// new compliance rows or delete non relevant rows
			clustersFromDB = append(clustersFromDB[:clusterIndex], clustersFromDB[clusterIndex+1:]...)
		}
		// delete objects that in the db but were not sent in the bundle (leaf hub sends only living resources)
		s.deleteSelectedComplianceRows(leafHubName, clustersPerPolicy.PolicyId, clustersFromDB)
		if err = s.db.UpdateEnforcementAndResourceVersion(s.complianceTableName, clustersPerPolicy.PolicyId,
			leafHubName, s.getEnforcement(clustersPerPolicy.RemediationAction), clustersPerPolicy.ResourceVersion); err != nil { // update enforcement and version of all rows with leafHub and policyId
			log.Println(err)
		}
		// keep this policy in db, should remove from db policies that were not sent in the bundle
		if policyIndex, err := getObjectIndex(policyIDsFromDB, clustersPerPolicy.PolicyId); err == nil { //policy exists
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}
	// remove all policies not in the bundle
	for _, policyId := range policyIDsFromDB {
		if err := s.db.DeleteAllComplianceRows(s.complianceTableName, policyId, leafHubName); err != nil {
			log.Println(err)
			// TODO retry
		}
	}
	log.Println(fmt.Sprintf("finished handling 'ClustersPerPolicy' bundle from leaf hub '%s', generation %d",
		leafHubName, receivedBundle.GetGeneration()))

	return nil
}

func (s *PoliciesTransportToDBSyncer) deleteSelectedComplianceRows(leafHubName string, policyId string,
	clusterNames []string) {
	for _, clusterName := range clusterNames {
		if err := s.db.DeleteComplianceRow(s.complianceTableName, policyId, clusterName, leafHubName); err != nil {
			log.Println(fmt.Sprintf("error removing object %s from table status.%s", clusterName,
				s.complianceTableName)) // failed to delete compliance row from DB //TODO retry
		}
	}
}

// if we got the the handler function, then the bundle generation is newer than what we have in memory
func (s *PoliciesTransportToDBSyncer) handleComplianceBundle(receivedBundle bundle.Bundle) error {
	leafHubName := receivedBundle.GetLeafHubName()
	log.Println(fmt.Sprintf("start handling 'ComplianceStatus' bundle from leaf hub '%s', generation %d",
		leafHubName, receivedBundle.GetGeneration()))

	complianceBundle := receivedBundle.(*bundle.ComplianceStatusBundle)
	if complianceBundle.BaseBundleGeneration > s.bundlesGenerationLogPerLeafHub[leafHubName].
		lastClustersPerPolicyBundleGeneration {
		return fmt.Errorf("waiting for base bundle %d to be handled, reschduling 'ComplianceStatus' bundle from "+
			"leaf hub '%s', generation %d", complianceBundle.BaseBundleGeneration, leafHubName,
			receivedBundle.GetGeneration())
	}
	policyIDsFromDB, err := s.db.GetPolicyIDsByLeafHub(s.complianceTableName, leafHubName)
	if err != nil {
		return err
	}
	for _, object := range receivedBundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus := object.(*statusbundle.PolicyComplianceStatus)
		nonCompliantClustersFromDB, err := s.db.GetNonCompliantClustersByLeafHubAndPolicy(s.complianceTableName,
			leafHubName, policyComplianceStatus.PolicyId) // includes both non_compliant and unknown
		if err != nil {
			return err
		}
		// we assume that 'ClustersPerPolicy' handler handles the addition or removal of clusters rows.
		// in this handler function, we handle only the existing clusters rows.

		// update in db non compliant clusters
		s.updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName, policyComplianceStatus.PolicyId, nonCompliant,
			policyComplianceStatus.ResourceVersion, policyComplianceStatus.NonCompliantClusters,
			nonCompliantClustersFromDB)

		// update in db unknown compliance clusters
		s.updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName, policyComplianceStatus.PolicyId, unknown,
			policyComplianceStatus.ResourceVersion, policyComplianceStatus.UnknownComplianceClusters,
			nonCompliantClustersFromDB)

		// other clusters are implicitly considered as compliant
		for _, clusterName := range nonCompliantClustersFromDB { // clusters left in the non compliant from db list
			if err := s.db.UpdateComplianceRow(s.complianceTableName, policyComplianceStatus.PolicyId, clusterName,
				leafHubName, compliant, policyComplianceStatus.ResourceVersion); err != nil { // change to compliant
				log.Println(err)
			}
		}
		// for policies that are found in the db but not in the bundle - all clusters are compliant (implicitly)
		if policyIndex, err := getObjectIndex(policyIDsFromDB, policyComplianceStatus.PolicyId); err == nil {
			policyIDsFromDB = append(policyIDsFromDB[:policyIndex], policyIDsFromDB[policyIndex+1:]...)
		}
	}
	// update policies not in the bundle - all is compliant
	for _, policyId := range policyIDsFromDB {
		if err := s.db.UpdatePolicyCompliance(s.complianceTableName, policyId, leafHubName, compliant); err != nil {
			log.Println(err)
			// TODO retry
		}
	}

	log.Println(fmt.Sprintf("finished handling 'ComplianceStatus' bundle from leaf hub '%s', generation %d",
		leafHubName, receivedBundle.GetGeneration()))

	return nil
}

func (s *PoliciesTransportToDBSyncer) updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName string,
	policyId string, compliance string, version string, targetClusterNames []string, clustersFromDB []string) {
	for _, clusterName := range targetClusterNames { // go over the target clusters
		if err := s.db.UpdateComplianceRow(s.complianceTableName, policyId, clusterName, leafHubName, compliance,
			version); err != nil {
			log.Println(err)
		}
		clusterIndex, err := getObjectIndex(clustersFromDB, clusterName)
		if err != nil {
			continue // if cluster not in the list, skip
		}
		clustersFromDB = append(clustersFromDB[:clusterIndex], clustersFromDB[clusterIndex+1:]...) //mark ad handled
	}
}

func (s *PoliciesTransportToDBSyncer) getEnforcement(remediationAction v1.RemediationAction) string {
	if strings.ToLower(string(remediationAction)) == inform {
		return inform
	} else if strings.ToLower(string(remediationAction)) == enforce {
		return enforce
	}
	return unknown
}
