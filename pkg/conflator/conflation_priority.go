package conflator

type conflationPriority uint8

// priority list of conflation unit.
const (
	ManagedClustersPriority               conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority             conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority      conflationPriority = iota // CompleteComplianceStatus = 2
	DeltaComplianceStatusPriority         conflationPriority = iota // DeltaComplianceStatus = 3
	MinimalComplianceStatusPriority       conflationPriority = iota // MinimalComplianceStatus = 4
	SubscriptionsStatusPriority           conflationPriority = iota // SubscriptionsStatus = 5
	ControlInfoPriority                   conflationPriority = iota // ControlInfo = 6
	LocalPolicySpecPriority               conflationPriority = iota // LocalPolicySpec = 7
	LocalClustersPerPolicyPriority        conflationPriority = iota // LocalClustersPerPolicy = 8
	LocalCompleteComplianceStatusPriority conflationPriority = iota // LocalCompleteComplianceStatus = 9
	LocalPlacementRulesSpecPriority       conflationPriority = iota // LocalPlacementRulesSpec = 10
)
