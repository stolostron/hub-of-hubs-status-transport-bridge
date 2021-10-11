package db

// db schemas.
const (
	// StatusSchema schema for status updates.
	StatusSchema = "status"
)

// table names.
const (
	// ManagedClustersTable table name of managed clusters.
	ManagedClustersTable = "managed_clusters"
	// ComplianceTable table name of policy compliance status.
	ComplianceTable = "compliance"
	// MinimalComplianceTable table name of minimal policy compliance status.
	MinimalComplianceTable = "aggregated_compliance"
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
