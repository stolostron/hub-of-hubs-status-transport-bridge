package dbsyncer

const (
	// ManagedClustersTableName table for managed clusters.
	managedClustersTableName = "status.managed_clusters"
	// ComplianceTableName table for policy compliance status.
	complianceTableName = "status.compliance"
	// MinimalComplianceTableName table for minimal policy compliance status.
	minimalComplianceTableName = "status.aggregated_compliance"

	// LocalComplianceTableName table for policy compliance status.
	LocalComplianceTableName = "local_status.compliance"
	// LocalPolicySpecTableName the name of the local policy spec table.
	LocalPolicySpecTableName = "local_spec.policies"
	// LocalPlacementRuleTableName the table for local Placement Rules.
	LocalPlacementRuleTableName = "local_spec.placementrules"
)
