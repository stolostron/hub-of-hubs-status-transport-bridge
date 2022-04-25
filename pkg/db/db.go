package db

import (
	"context"

	set "github.com/deckarep/golang-set"
)

// StatusTransportBridgeDB is the db interface required by status transport bridge.
type StatusTransportBridgeDB interface {
	GetPoolSize() int32
	Stop()

	ManagedClustersStatusDB
	PoliciesStatusDB
	PoliciesPlacementStatusDB
	AggregatedPoliciesStatusDB
	GenericStatusResourceDB
	LocalPoliciesStatusDB
	ControlInfoDB
}

// BatchSenderDB is the db interface required for sending batch updates.
type BatchSenderDB interface {
	SendBatch(ctx context.Context, batch interface{}) error
}

// ManagedClustersStatusDB is the db interface required to manage managed clusters status.
type ManagedClustersStatusDB interface {
	BatchSenderDB
	// GetManagedClustersByLeafHub returns a map from clusterName to its resourceVersion.
	GetManagedClustersByLeafHub(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]string, error)
	// NewManagedClustersBatchBuilder returns managed clusters batch builder.
	NewManagedClustersBatchBuilder(schema string, tableName string, leafHubName string) ManagedClustersBatchBuilder
}

// PoliciesStatusDB is the db interface required to manage policies status.
type PoliciesStatusDB interface {
	BatchSenderDB
	// GetComplianceStatusByLeafHub returns a map of policies, each maps to a set of clusters.
	GetComplianceStatusByLeafHub(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]*PolicyClustersSets, error)
	// GetNonCompliantClustersByLeafHub returns a map of policies, each maps to sets of (NonCompliant,Unknown) clusters.
	GetNonCompliantClustersByLeafHub(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]*PolicyClustersSets, error)
	// NewPoliciesBatchBuilder returns policies status batch builder.
	NewPoliciesBatchBuilder(schema string, tableName string, leafHubName string) PoliciesBatchBuilder
}

// PoliciesPlacementStatusDB is the db interface required to manage policies placement status.
type PoliciesPlacementStatusDB interface {
	BatchSenderDB
	// GetPoliciesPlacementByLeafHub returns a map from policyID to its resourceVersion.
	GetPoliciesPlacementByLeafHub(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]string, error)
	// NewPoliciesPlacementBatchBuilder returns policies placement batch builder.
	NewPoliciesPlacementBatchBuilder(schema string, tableName string, leafHubName string) PoliciesPlacementBatchBuilder
}

// AggregatedPoliciesStatusDB is the db interface required to manage aggregated policy info.
type AggregatedPoliciesStatusDB interface {
	GetPolicyIDsByLeafHub(ctx context.Context, schema string, tableName string, leafHubName string) (set.Set, error)
	InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, schema string, tableName string, leafHubName string,
		policyID string, appliedClusters int, nonCompliantClusters int) error
	DeleteAllComplianceRows(ctx context.Context, schema string, tableName string, leafHubName string,
		policyID string) error
}

// GenericStatusResourceDB is the db interface required to manage generic status resources.
type GenericStatusResourceDB interface {
	BatchSenderDB
	// GetDistinctIDAndVersion returns a map from resource id to its resourceVersion.
	GetDistinctIDAndVersion(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]string, error)
	NewGenericBatchBuilder(schema string, tableName string, leafHubName string) GenericBatchBuilder
}

// LocalPoliciesStatusDB is the db interface required to manage local policies.
type LocalPoliciesStatusDB interface {
	BatchSenderDB
	// GetLocalDistinctIDAndVersion returns a map from resource id to its resourceVersion.
	GetLocalDistinctIDAndVersion(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]string, error)
	// NewGenericLocalBatchBuilder returns generic local batch builder.
	NewGenericLocalBatchBuilder(schema string, tableName string, leafHubName string) GenericLocalBatchBuilder
}

// ControlInfoDB is the db interface required to manage control info status.
type ControlInfoDB interface {
	// UpdateHeartbeat inserts or updates heartbeat for a leaf hub.
	UpdateHeartbeat(ctx context.Context, schema string, tableName string, leafHubName string) error
}
