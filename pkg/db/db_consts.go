package db

// db schemas.
const (
	// StatusSchema schema for status updates.
	StatusSchema = "status"
	// LocalStatusSchema schema for local status updates.
	LocalStatusSchema = "local_status"
	// LocalSpecSchema schema for local spec updates.
	LocalSpecSchema = "local_spec"
)

// table names.
const (
	// ManagedClustersTable table name of managed clusters.
	ManagedClustersTable = "managed_clusters"
	// ComplianceTable table name of policy compliance status.
	ComplianceTable = "compliance"
	// MinimalComplianceTable table name of minimal policy compliance status.
	MinimalComplianceTable = "aggregated_compliance"
	// LocalPolicySpecTableName table name of local policy spec.
	LocalPolicySpecTableName = "policies"
	// LocalPlacementRulesTableName table name of local placement rules.
	LocalPlacementRulesTableName = "placementrules"
	// LeafHubHeartbeatsTableName table name for LH heartbeats.
	LeafHubHeartbeatsTableName = "leaf_hub_heartbeats"
	// SubscriptionTableName table name of the subscriptions table.
	SubscriptionTableName = "subscriptions"
)

// default values.
const (
	// ErrorNone is default value when no error occurs.
	ErrorNone = "none"
)

// ComplianceStatus represents the possible options for compliance status.
type ComplianceStatus string

// compliance states.
const (
	// NonCompliant non compliant state.
	NonCompliant ComplianceStatus = "non_compliant"
	// Compliant compliant state.
	Compliant ComplianceStatus = "compliant"
	// Unknown unknown compliance state.
	Unknown ComplianceStatus = "unknown"
)

// unique db types.
const (
	// uuid unique type.
	UUID = "uuid"
	// jsonb unique type.
	Jsonb = "jsonb"
	// compliance type unique type.
	StatusComplianceType = "status.compliance_type"
)
