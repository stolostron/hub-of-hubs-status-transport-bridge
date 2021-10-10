package bundle

import (
	"fmt"

	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/datastructures"
)

const (
	completeComplianceStatusBundleType = "CompleteComplianceStatusBundle"
)

// NewDeltaComplianceStatusBundle creates a new instance of DeltaComplianceStatusBundle.
func NewDeltaComplianceStatusBundle() *DeltaComplianceStatusBundle {
	return &DeltaComplianceStatusBundle{}
}

// DeltaComplianceStatusBundle abstracts management of compliance status bundle.
type DeltaComplianceStatusBundle struct {
	statusbundle.BaseDeltaComplianceStatusBundle
}

// InheritContent updates the content of this bundle with that of another (this bundle is the source of truth).
func (bundle *DeltaComplianceStatusBundle) InheritContent(olderBundle Bundle) error {
	oldDeltaComplianceBundle, ok := olderBundle.(*DeltaComplianceStatusBundle)
	if !ok {
		return fmt.Errorf("%w - expecting %s", errWrongType, "DeltaComplianceStatusBundle")
	}

	bundle.inheritObjects(oldDeltaComplianceBundle.Objects)

	return nil
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *DeltaComplianceStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *DeltaComplianceStatusBundle) GetObjects() []interface{} {
	result := make([]interface{}, len(bundle.Objects))

	for i, obj := range bundle.Objects {
		result[i] = obj
	}

	return result
}

// GetVersion returns the bundle version.
func (bundle *DeltaComplianceStatusBundle) GetVersion() *statusbundle.BundleVersion {
	return &bundle.BundleVersion
}

// GetDependency return the bundle dependency or nil in case there is no dependency.
func (bundle *DeltaComplianceStatusBundle) GetDependency() *DependencyBundle {
	return &DependencyBundle{
		RequiresExactVersion: true,
		BundleType:           completeComplianceStatusBundleType,
		BundleVersion:        statusbundle.NewBundleVersion(bundle.Incarnation, bundle.BaseBundleGeneration),
	}
}

type policyInfo struct {
	compliantClusters    *datastructures.HashSet
	nonCompliantClusters *datastructures.HashSet
	unknownClusters      *datastructures.HashSet
}

func (bundle *DeltaComplianceStatusBundle) inheritObjects(oldObjects []*statusbundle.PolicyDeltaComplianceStatus) {
	policiesMap := make(map[string]*policyInfo, len(bundle.Objects))
	survivingOldPolicies := make([]*statusbundle.PolicyDeltaComplianceStatus, 0, len(oldObjects))

	// create policy-info for my policies
	for _, policy := range bundle.Objects {
		compliantClusters, nonCompliantClusters,
			unknownClusters := bundle.mapComplianceAndClusters(policy.CompliantClusters,
			policy.NonCompliantClusters, policy.UnknownComplianceClusters)

		policiesMap[policy.PolicyID] = &policyInfo{
			compliantClusters:    &compliantClusters,
			nonCompliantClusters: &nonCompliantClusters,
			unknownClusters:      &unknownClusters,
		}
	}

	// go over old bundle policies and add those missing / clusters whose statuses were not mapped
	for _, policy := range oldObjects {
		policyInfo, found := policiesMap[policy.PolicyID]
		if !found {
			// policy was not mapped, add it whole
			survivingOldPolicies = append(survivingOldPolicies, policy)

			continue
		}

		// policy exists in map, add clusters that do not exist currently
		bundle.updatePolicyInfoWithNewClusters(policyInfo, policy)
	}

	// update my policies
	for _, policy := range bundle.Objects {
		policiesMap[policy.PolicyID].updatePolicyDeltaComplianceStatus(policy)
	}

	// update bundle's objects with the surviving policies + new updated policies
	bundle.Objects = append(survivingOldPolicies, bundle.Objects...)
}

func (bundle *DeltaComplianceStatusBundle) mapComplianceAndClusters(compliantClustersSlice, nonCompliantClustersSlice,
	unknownClustersSlice []string) (datastructures.HashSet, datastructures.HashSet, datastructures.HashSet) {
	compliantClusters := datastructures.NewHashSet(len(compliantClustersSlice))
	nonCompliantClusters := datastructures.NewHashSet(len(nonCompliantClustersSlice))
	unknownClusters := datastructures.NewHashSet(len(unknownClustersSlice))

	for _, cluster := range compliantClustersSlice {
		compliantClusters.Add(cluster)
	}

	for _, cluster := range nonCompliantClustersSlice {
		nonCompliantClusters.Add(cluster)
	}

	for _, cluster := range unknownClustersSlice {
		unknownClusters.Add(cluster)
	}

	return compliantClusters, nonCompliantClusters, unknownClusters
}

func (bundle *DeltaComplianceStatusBundle) updatePolicyInfoWithNewClusters(policyInfo *policyInfo,
	policy *statusbundle.PolicyDeltaComplianceStatus) {
	for _, cluster := range policy.CompliantClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.compliantClusters.Add(cluster)
		}
	}

	for _, cluster := range policy.NonCompliantClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.nonCompliantClusters.Add(cluster)
		}
	}

	for _, cluster := range policy.UnknownComplianceClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.unknownClusters.Add(cluster)
		}
	}
}

func (pi *policyInfo) exists(cluster string) bool {
	return pi.compliantClusters.Exists(cluster) || pi.nonCompliantClusters.Exists(cluster) ||
		pi.unknownClusters.Exists(cluster)
}

func (pi *policyInfo) updatePolicyDeltaComplianceStatus(policy *statusbundle.PolicyDeltaComplianceStatus) {
	compliantClustersSlice := make([]string, 0, len(*pi.compliantClusters))
	nonCompliantClustersSlice := make([]string, 0, len(*pi.nonCompliantClusters))
	unknownClustersSlice := make([]string, 0, len(*pi.unknownClusters))

	for cluster := range *pi.compliantClusters {
		compliantClustersSlice = append(compliantClustersSlice, cluster)
	}

	for cluster := range *pi.nonCompliantClusters {
		nonCompliantClustersSlice = append(nonCompliantClustersSlice, cluster)
	}

	for cluster := range *pi.unknownClusters {
		unknownClustersSlice = append(unknownClustersSlice, cluster)
	}

	policy.CompliantClusters = compliantClustersSlice
	policy.NonCompliantClusters = nonCompliantClustersSlice
	policy.UnknownComplianceClusters = unknownClustersSlice
}
