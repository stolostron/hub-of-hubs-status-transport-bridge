package dbsyncer

const (
	// ManagedClustersTableName table for managed clusters.
	ManagedClustersTableName = "managed_clusters"
	// ComplianceTableName table for policy compliance status.
	ComplianceTableName = "compliance"
	// MinimalComplianceTableName table for minimal policy compliance status.
	MinimalComplianceTableName = "aggregated_compliance"

	// LocalComplianceTableName table for policy compliance status.
	LocalComplianceTableName = "local_compliance"
	// LocalPolicySpecTableName the name of the local policy spec table.
	LocalPolicySpecTableName = "local_policies"
	// LocalPlacementRuleTableName the table for local Placement Rules.
	LocalPlacementRuleTableName = "local_placementrules"
)
