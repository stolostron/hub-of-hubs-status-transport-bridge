package conflator

type conflationPriority uint

// iota is the golang way of implementing ENUM

// priority list of conflation unit.
const (
	ManagedClustersPriority          conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority        conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority conflationPriority = iota // CompleteComplianceStatus = 2
	MinimalComplianceStatusPriority  conflationPriority = iota // MinimalComplianceStatus = 3
	LocalPolicySpecPriority          conflationPriority = iota // LocalPolicySpecPriority = 3
	LocalPlacementRuleSpecPriority   conflationPriority = iota // LocalPlacementRuleSpecPriority = 4
	LocalComplianceStatusPriority    conflationPriority = iota // LocalComplianceStatus = 5.
	LocalClustersPerPolicyPriority   conflationPriority = iota // LocalClustersPerPolicy = 6.
)
