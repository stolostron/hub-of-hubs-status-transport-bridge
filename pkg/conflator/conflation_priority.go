package conflator

type conflationPriority uint

// iota is the golang way of implementing ENUM

// priority list of conflation unit.
const (
	ManagedClustersPriority         conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority       conflationPriority = iota // ClustersPerPolicy = 1
	ComplianceStatusPriority        conflationPriority = iota // ComplianceStatus = 2
	MinimalComplianceStatusPriority conflationPriority = iota // MinimalComplianceStatus = 3
	SubscriptionStatusPriority      conflationPriority = iota // SubscriptionStatusPriority = 4
)
