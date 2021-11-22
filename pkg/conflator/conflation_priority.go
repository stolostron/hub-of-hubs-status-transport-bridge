package conflator

type conflationPriority uint

// iota is the golang way of implementing ENUM

// priority list of conflation unit.
const (
	ManagedClustersPriority          conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority        conflationPriority = iota // ClustersPerPolicy = 1
	CompleteComplianceStatusPriority conflationPriority = iota // CompleteComplianceStatus = 2
	DeltaComplianceStatusPriority    conflationPriority = iota // DeltaComplianceStatus = 3
	MinimalComplianceStatusPriority  conflationPriority = iota // MinimalComplianceStatus = 4
)
