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
	AggregatedPoliciesStatusDB
	GenericDBTransport
}

// BatchSenderDB is the db interface required for sending batch updates.
type BatchSenderDB interface {
	SendBatch(ctx context.Context, batch interface{}) error
}

// ManagedClustersStatusDB is the db interface required by status transport bridge to manage managed clusters status.
type ManagedClustersStatusDB interface {
	BatchSenderDB
	// GetManagedClustersByLeafHub returns a map from of clusterName to its resourceVersion.
	GetManagedClustersByLeafHub(ctx context.Context, schema string, tableName string,
		leafHubName string) (map[string]string, error)
	// NewManagedClustersBatchBuilder returns managed clusters batch builder.
	NewManagedClustersBatchBuilder(schema string, tableName string, leafHubName string) ManagedClustersBatchBuilder
}

// PoliciesStatusDB is the db interface required by status transport bridge to manage policy status.
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

// AggregatedPoliciesStatusDB is the db interface required by status transport bridge to manage aggregated policy info.
type AggregatedPoliciesStatusDB interface {
	GetPolicyIDsByLeafHub(ctx context.Context, schema string, tableName string, leafHubName string) (set.Set, error)
	InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, schema string, tableName string, leafHubName string,
		policyID string, appliedClusters int, nonCompliantClusters int) error
	DeleteAllComplianceRows(ctx context.Context, schema string, tableName string, leafHubName string,
		policyID string) error
}

// GenericDBTransport is the db interface required to manage generic data with the db.
type GenericDBTransport interface {
	BatchSenderDB
	GetDistinctIDsFromLH(ctx context.Context, schema string, tableName string, leafHubName string) ([]string, error)
	NewGenericBatchBuilder(schema string, tableName string, leafHubName string) GenericBatchBuilder
}
