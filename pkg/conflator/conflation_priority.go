package conflator

// ConflationPriority sets priority of processing for the conflators.
type ConflationPriority uint8

// priority list of conflation unit.
const (
	ManagedClustersPriority               ConflationPriority = iota
	ClustersPerPolicyPriority             ConflationPriority = iota
	CompleteComplianceStatusPriority      ConflationPriority = iota
	DeltaComplianceStatusPriority         ConflationPriority = iota
	PoliciesPlacementPriority             ConflationPriority = iota
	MinimalComplianceStatusPriority       ConflationPriority = iota
	PlacementRulePriority                 ConflationPriority = iota
	PlacementPriority                     ConflationPriority = iota
	PlacementDecisionPriority             ConflationPriority = iota
	SubscriptionStatusPriority            ConflationPriority = iota
	SubscriptionReportPriority            ConflationPriority = iota
	ControlInfoPriority                   ConflationPriority = iota
	LocalPolicySpecPriority               ConflationPriority = iota
	LocalClustersPerPolicyPriority        ConflationPriority = iota
	LocalCompleteComplianceStatusPriority ConflationPriority = iota
	LocalPlacementRulesSpecPriority       ConflationPriority = iota
)
