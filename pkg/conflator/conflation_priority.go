package conflator

type conflationPriority int

// iota is the golang way of implementing ENUM

// priority list of conflation unit.
const (
	ManagedClustersPriority         conflationPriority = iota // ManagedClusters = 0
	ClustersPerPolicyPriority                          = iota // ClustersPerPolicy = 1
	ComplianceStatusPriority                           = iota // ComplianceStatus = 2
	MinimalComplianceStatusPriority                    = iota // MinimalComplianceStatus = 3
)
