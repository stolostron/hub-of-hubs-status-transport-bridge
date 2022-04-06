package conflator

type conflationPriority uint8

// priority list of conflation unit.
const (
	ManagedClustersPriority               conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority             conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority      conflationPriority = iota // CompleteComplianceStatus = 2
	DeltaComplianceStatusPriority         conflationPriority = iota // DeltaComplianceStatus = 3
	PoliciesPlacementPriority             conflationPriority = iota // PoliciesPlacement = 4
	MinimalComplianceStatusPriority       conflationPriority = iota // MinimalComplianceStatus = 5
	SubscriptionsStatusPriority           conflationPriority = iota // SubscriptionsStatus = 6
	ControlInfoPriority                   conflationPriority = iota // ControlInfo = 7
	LocalPolicySpecPriority               conflationPriority = iota // LocalPolicySpec = 8
	LocalClustersPerPolicyPriority        conflationPriority = iota // LocalClustersPerPolicy = 9
	LocalCompleteComplianceStatusPriority conflationPriority = iota // LocalCompleteComplianceStatus = 10
	LocalPlacementRulesSpecPriority       conflationPriority = iota // LocalPlacementRulesSpec = 11
)
