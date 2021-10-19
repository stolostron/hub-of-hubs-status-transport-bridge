package db

// db schemas.
const (
	// StatusSchema schema for status updates.
	StatusSchema = "status"
	// LocalStatusScheme schema for local status updates.
	LocalStatusScheme = "local_status"
	// LocalSpecScheme schema for local spec updates.
	LocalSpecScheme = "local_spec"
)

// table names.
const (
	// ManagedClustersTable table name of managed clusters.
	ManagedClustersTable = "managed_clusters"
	// ComplianceTable table name of policy compliance status.
	ComplianceTable = "compliance"
	// MinimalComplianceTable table name of minimal policy compliance status.
	MinimalComplianceTable = "aggregated_compliance"
	// LocalComplianceTableName table for policy compliance status.
	LocalComplianceTableName = "compliance"
	// LocalPolicySpecTableName the name of the local policy spec table.
	LocalPolicySpecTableName = "policies"
	// LocalPlacementRuleTableName the table for local Placement Rules.
	LocalPlacementRuleTableName = "placementrules"
)

// default values.
const (
	// ErrorNone is default value when no error occurs.
	ErrorNone = "none"
)

// compliance states.
const (
	// NonCompliant non compliant state.
	NonCompliant = "non_compliant"
	// Compliant compliant state.
	Compliant = "compliant"
	// Unknown unknown compliance state.
	Unknown = "unknown"
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
