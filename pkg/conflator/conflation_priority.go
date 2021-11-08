package conflator

type conflationPriority uint

// iota is the golang way of implementing ENUM

// priority list of conflation unit.
const (
	ManagedClustersPriority               conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority             conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority      conflationPriority = iota // CompleteComplianceStatus = 2
	MinimalComplianceStatusPriority       conflationPriority = iota // MinimalComplianceStatus = 3
	LocalPolicySpecPriority               conflationPriority = iota // LocalPolicySpecPriority = 4
	LocalClustersPerPolicyPriority        conflationPriority = iota // LocalClustersPerPolicy = 5
	LocalCompleteComplianceStatusPriority conflationPriority = iota // LocalComplianceStatus = 6
	LocalPlacementRuleSpecPriority        conflationPriority = iota // LocalPlacementRuleSpecPriority = 7
)
