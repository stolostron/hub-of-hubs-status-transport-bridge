package conflator

type conflationPriority uint8

// priority list of conflation unit.
const (
	ManagedClustersPriority               conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority             conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority      conflationPriority = iota // CompleteComplianceStatus = 2
	DeltaComplianceStatusPriority         conflationPriority = iota // DeltaComplianceStatus = 3
	MinimalComplianceStatusPriority       conflationPriority = iota // MinimalComplianceStatus = 4
	ControlInfoPriority                   conflationPriority = iota // ControlInfo = 5
	LocalPolicySpecPriority               conflationPriority = iota // LocalPolicySpec = 6
	LocalClustersPerPolicyPriority        conflationPriority = iota // LocalClustersPerPolicy = 7
	LocalCompleteComplianceStatusPriority conflationPriority = iota // LocalCompleteComplianceStatus = 8
	LocalPlacementRulesSpecPriority       conflationPriority = iota // LocalPlacementRulesSpec = 9
	SubscriptionStatusPriority            conflationPriority = iota // SubscriptionStatus = 10
)
