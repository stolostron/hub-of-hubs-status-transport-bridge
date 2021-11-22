package bundle

import (
	"fmt"

	statusbundle "github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
)

// NewDeltaComplianceStatusBundle creates a new instance of DeltaComplianceStatusBundle.
func NewDeltaComplianceStatusBundle() *DeltaComplianceStatusBundle {
	return &DeltaComplianceStatusBundle{
		dependencyVersion: nil,
	}
}

// DeltaComplianceStatusBundle abstracts management of compliance status bundle.
type DeltaComplianceStatusBundle struct {
	statusbundle.BaseDeltaComplianceStatusBundle
	dependencyVersion *statusbundle.BundleVersion
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

// GetDependencyVersion returns the bundle dependency required version.
func (bundle *DeltaComplianceStatusBundle) GetDependencyVersion() *statusbundle.BundleVersion {
	// incarnation version of the dependency bundle has to be equal to that of the dependent bundle (same sender),
	// therefore it is omitted from the bundle itself (only generation is maintained).
	if bundle.dependencyVersion == nil {
		bundle.dependencyVersion = statusbundle.NewBundleVersion(bundle.Incarnation, bundle.BaseBundleGeneration)
	}

	return bundle.dependencyVersion
}

type policyInfo struct {
	compliantClusters    map[string]struct{}
	nonCompliantClusters map[string]struct{}
	unknownClusters      map[string]struct{}
}

func (bundle *DeltaComplianceStatusBundle) inheritObjects(oldObjects []*statusbundle.PolicyGenericComplianceStatus) {
	policiesMap := make(map[string]*policyInfo, len(bundle.Objects))
	survivingOldPolicies := make([]*statusbundle.PolicyGenericComplianceStatus, 0, len(oldObjects))

	// create policy-info for my policies
	for _, policy := range bundle.Objects {
		compliantClusters, nonCompliantClusters,
			unknownClusters := bundle.mapComplianceAndClusters(policy.CompliantClusters,
			policy.NonCompliantClusters, policy.UnknownComplianceClusters)

		policiesMap[policy.PolicyID] = &policyInfo{
			compliantClusters:    compliantClusters,
			nonCompliantClusters: nonCompliantClusters,
			unknownClusters:      unknownClusters,
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
		bundle.updatePolicyInfoWithNewClustersFromOldBundle(policyInfo, policy)
	}

	// update my policies
	for _, policy := range bundle.Objects {
		policiesMap[policy.PolicyID].updatePolicyDeltaComplianceStatus(policy)
	}

	// update bundle's objects with the surviving policies + new updated policies
	bundle.Objects = append(survivingOldPolicies, bundle.Objects...)
}

func (bundle *DeltaComplianceStatusBundle) mapComplianceAndClusters(compliantClustersSlice, nonCompliantClustersSlice,
	unknownClustersSlice []string) (map[string]struct{}, map[string]struct{}, map[string]struct{}) {
	compliantClusters := make(map[string]struct{}, len(compliantClustersSlice))
	nonCompliantClusters := make(map[string]struct{}, len(nonCompliantClustersSlice))
	unknownClusters := make(map[string]struct{}, len(unknownClustersSlice))

	for _, cluster := range compliantClustersSlice {
		compliantClusters[cluster] = struct{}{}
	}

	for _, cluster := range nonCompliantClustersSlice {
		nonCompliantClusters[cluster] = struct{}{}
	}

	for _, cluster := range unknownClustersSlice {
		unknownClusters[cluster] = struct{}{}
	}

	return compliantClusters, nonCompliantClusters, unknownClusters
}

// updatePolicyInfoWithNewClustersFromOldBundle gets a policy from an old bundle and updates the policyInfo with
// clusters that do not exist currently within any list. (e.g. if a cluster is now compliant and in the old policy
// received it is non-compliant, nothing happens).
func (bundle *DeltaComplianceStatusBundle) updatePolicyInfoWithNewClustersFromOldBundle(policyInfo *policyInfo,
	policy *statusbundle.PolicyGenericComplianceStatus) {
	for _, cluster := range policy.CompliantClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.compliantClusters[cluster] = struct{}{}
		}
	}

	for _, cluster := range policy.NonCompliantClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.nonCompliantClusters[cluster] = struct{}{}
		}
	}

	for _, cluster := range policy.UnknownComplianceClusters {
		if !policyInfo.exists(cluster) {
			policyInfo.unknownClusters[cluster] = struct{}{}
		}
	}
}

func (pi *policyInfo) exists(cluster string) bool {
	_, foundInCompliant := pi.compliantClusters[cluster]
	_, foundInNonCompliant := pi.nonCompliantClusters[cluster]
	_, foundInUnknown := pi.unknownClusters[cluster]

	return foundInCompliant || foundInNonCompliant || foundInUnknown
}

func (pi *policyInfo) updatePolicyDeltaComplianceStatus(policy *statusbundle.PolicyGenericComplianceStatus) {
	compliantClustersSlice := make([]string, 0, len(pi.compliantClusters))
	nonCompliantClustersSlice := make([]string, 0, len(pi.nonCompliantClusters))
	unknownClustersSlice := make([]string, 0, len(pi.unknownClusters))

	for cluster := range pi.compliantClusters {
		compliantClustersSlice = append(compliantClustersSlice, cluster)
	}

	for cluster := range pi.nonCompliantClusters {
		nonCompliantClustersSlice = append(nonCompliantClustersSlice, cluster)
	}

	for cluster := range pi.unknownClusters {
		unknownClustersSlice = append(unknownClustersSlice, cluster)
	}

	policy.CompliantClusters = compliantClustersSlice
	policy.NonCompliantClusters = nonCompliantClustersSlice
	policy.UnknownComplianceClusters = unknownClustersSlice
}
