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
)

const (
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
		db:                                    db,
		managedClustersTableName:              managedClustersTableName,
		complianceTableName:                   complianceTableName,
		clustersPerPolicyBundleUpdatesChan:    make(chan bundle.Bundle),
		complianceBundleUpdatesChan:           make(chan bundle.Bundle),
		lastClustersPerPolicyBundleGeneration: 0,
		lastComplianceBundleGeneration:        0,
		stopChan:                              stopChan,
	}
	registerToBundleUpdates(transport, clusterPerPolicyRegistration, syncer.clustersPerPolicyBundleUpdatesChan)
	registerToBundleUpdates(transport, complianceRegistration, syncer.complianceBundleUpdatesChan)
	log.Println(fmt.Sprintf("initialized syncer for table status.%s", complianceTableName))
	return syncer
}

type PoliciesTransportToDBSyncer struct {
	db                                    hohDb.StatusTransportBridgeDB
	managedClustersTableName              string
	complianceTableName                   string
	clustersPerPolicyBundleUpdatesChan    chan bundle.Bundle
	complianceBundleUpdatesChan           chan bundle.Bundle
	lastClustersPerPolicyBundleGeneration uint64
	lastComplianceBundleGeneration        uint64
	stopChan                              chan struct{}
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
			if err := handleBundle(clustersPerPolicyBundle, &s.lastClustersPerPolicyBundleGeneration,
				s.handleClustersPerPolicyBundle); err != nil {
				log.Println(err)
				// TODO schedule a retry, use exponential backoff
			}
		case complianceBundle := <-s.complianceBundleUpdatesChan:
			if err := handleBundle(complianceBundle, &s.lastComplianceBundleGeneration, s.handleComplianceBundle); err != nil {
				log.Println(err)
				// TODO schedule a retry, use exponential backoff
			}
		}
	}
}

// if we got inside the handler function, then the bundle generation is newer than what we have in memory
// handling bundle clusters per policy only inserts or deletes rows from/to the compliance table.
// in case the row already exists (leafHubName, policyId, clusterName) -> then don't change anything since this bundle
// don't have any information about the compliance status but only for the list of relevant clusters.
// the compliance status will be handled in a different bundle and a different handler function
func (s *PoliciesTransportToDBSyncer) handleClustersPerPolicyBundle(bundle bundle.Bundle) error {
	leafHubName := bundle.GetLeafHubName()
	policyIDsFromDB, err := s.db.GetPolicyIDsByLeafHub(s.complianceTableName, leafHubName)
	if err != nil {
		return err
	}
	for _, object := range bundle.GetObjects() { // every object in bundle is clusters list per policy
		clustersPerPolicy := object.(*statusbundle.ClustersPerPolicy)
		clustersFromDB, err := s.db.GetComplianceClustersByLeafHubAndPolicy(s.complianceTableName, leafHubName,
			clustersPerPolicy.PolicyId)
		if err != nil {
			return err
		}
		for _, clusterName := range clustersPerPolicy.Clusters { // go over the received bundle clusters per policy
			// TODO lock row of managed cluster in the clusters table, free when update is finished
			if !s.db.ManagedClusterExists(s.managedClustersTableName, leafHubName, clusterName) {
				// TODO schedule retry
				continue // cluster name doesn't exist in the managed clusters status table
			}
			clusterIndex, err := getObjectIndex(clustersFromDB, clusterName)
			if err != nil { // cluster not found in the compliance table (and exists in managed clusters status table)
				if err = s.db.InsertPolicyCompliance(s.complianceTableName, clustersPerPolicy.PolicyId, clusterName,
					leafHubName, clustersPerPolicy.ResourceVersion); err != nil {
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
		if err = s.db.UpdateResourceVersion(s.complianceTableName, clustersPerPolicy.PolicyId, leafHubName,
			clustersPerPolicy.ResourceVersion); err != nil { // update version of all rows with leafHub and policyId
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
		leafHubName, bundle.GetGeneration()))

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
func (s *PoliciesTransportToDBSyncer) handleComplianceBundle(bundle bundle.Bundle) error {
	// TODO at the moment, assume the base was handled, later will add waiting time if it didn't
	leafHubName := bundle.GetLeafHubName()
	for _, object := range bundle.GetObjects() { // every object in bundle is policy compliance status
		policyComplianceStatus := object.(*statusbundle.PolicyComplianceStatus)
		clustersFromDB, err := s.db.GetComplianceClustersByLeafHubAndPolicy(s.complianceTableName, leafHubName,
			policyComplianceStatus.PolicyId)
		if err != nil {
			return err
		}
		// we assume that 'ClustersPerPolicy' handler handles the addition or removal of clusters rows.
		// in this handler function, we handle only the existing clusters rows.

		// update in db non compliant clusters
		s.updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName, policyComplianceStatus.PolicyId, nonCompliant,
			s.getEnforcement(policyComplianceStatus.RemediationAction), policyComplianceStatus.ResourceVersion,
			policyComplianceStatus.NonCompliantClusters, clustersFromDB)

		// update in db unknown compliance clusters
		s.updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName, policyComplianceStatus.PolicyId, unknown,
			s.getEnforcement(policyComplianceStatus.RemediationAction), policyComplianceStatus.ResourceVersion,
			policyComplianceStatus.UnknownComplianceClusters, clustersFromDB)

		// other clusters are implicitly considered as compliant
		if err = s.db.UpdateComplianceRowsWithLowerVersion(s.complianceTableName, policyComplianceStatus.PolicyId,
			leafHubName, compliant, s.getEnforcement(policyComplianceStatus.RemediationAction),
			policyComplianceStatus.ResourceVersion); err != nil {
			log.Println(err)
		}
	}
	log.Println(fmt.Sprintf("finished handling 'ComplianceStatus' bundle from leaf hub '%s', generation %d",
		leafHubName, bundle.GetGeneration()))

	return nil
}

func (s *PoliciesTransportToDBSyncer) updateSelectedComplianceRowsAndRemovedFromDBList(leafHubName string,
	policyId string, compliance string, enforcement string, version string, targetClusterNames []string,
	clustersFromDB []string) {
	for _, clusterName := range targetClusterNames { // go over the target clusters
		clusterIndex, err := getObjectIndex(clustersFromDB, clusterName)
		if err != nil {
			continue // if cluster not found in the compliance table, cannot update it's compliance status
		}
		if err = s.db.UpdateComplianceRow(s.complianceTableName, policyId, clusterName,
			leafHubName, compliance, enforcement, version); err != nil {
			log.Println(err)
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
