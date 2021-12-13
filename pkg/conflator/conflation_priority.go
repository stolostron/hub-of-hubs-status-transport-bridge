package conflator

type conflationPriority uint8

// priority list of conflation unit.
const (
	ManagedClustersPriority               conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority             conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority      conflationPriority = iota // CompleteComplianceStatus = 2
	MinimalComplianceStatusPriority       conflationPriority = iota // MinimalComplianceStatus = 3
	ControlInfoPriority                   conflationPriority = iota // ControlInfo = 4
	LocalPolicySpecPriority               conflationPriority = iota // LocalPolicySpec = 5
	LocalClustersPerPolicyPriority        conflationPriority = iota // LocalClustersPerPolicy = 6
	LocalCompleteComplianceStatusPriority conflationPriority = iota // LocalCompleteComplianceStatus = 7
	LocalPlacementRulesSpecPriority       conflationPriority = iota // LocalPlacementRulesSpec = 8
	SubscriptionStatusPriority            conflationPriority = iota // SubscriptionStatusPriority = 9
)
