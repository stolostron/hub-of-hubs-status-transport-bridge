package dbsyncer

const (
	// ManagedClustersTableName table for managed clusters.
	managedClustersTableName = "status.managed_clusters"
	// ComplianceTableName table for policy compliance status.
	complianceTableName = "status.compliance"
	// MinimalComplianceTableName table for minimal policy compliance status.
	minimalComplianceTableName = "status.aggregated_compliance"

	// // LocalComplianceTableName table for policy compliance status.
	// localComplianceTableName = "local_status.compliance"
	// // LocalPolicySpecTableName the name of the local policy spec table.
	// localPolicySpecTableName = "local_spec.policies"
	// // LocalPlacementRuleTableName the table for local Placement Rules.
	// localPlacementRuleTableName = "local_spec.placementrules"
	// SubscriptionTableName the table for application subscriptions.
	subscriptionTableName = "status.subscriptions"
)
