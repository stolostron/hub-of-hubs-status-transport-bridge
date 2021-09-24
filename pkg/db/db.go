package db

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/datastructures"
)

// StatusTransportBridgeDB is the db interface required by status transport bridge.
type StatusTransportBridgeDB interface {
	ManagedClustersStatusDB
	PoliciesStatusDB

	GetPoolSize() int32
	Stop()
}

// ManagedClustersStatusDB is the db interface required by status transport bridge to manage managed clusters status.
type ManagedClustersStatusDB interface {
	GetManagedClustersByLeafHub(ctx context.Context, tableName string, leafHubName string) (datastructures.HashSet,
		error)
	InsertManagedCluster(ctx context.Context, tableName string, leafHubName string, clusterName string,
		payload interface{}, version string) error
	UpdateManagedCluster(ctx context.Context, tableName string, leafHubName string, clusterName string,
		payload interface{}, version string) error
	DeleteManagedCluster(ctx context.Context, tableName string, leafHubName string, clusterName string) error
}

// PoliciesStatusDB is the db interface required by status transport bridge to manage policy status.
type PoliciesStatusDB interface {
	GetPolicyIDsByLeafHub(ctx context.Context, tableName string, leafHubName string) (datastructures.HashSet, error)
	GetComplianceClustersByLeafHubAndPolicy(ctx context.Context, tableName string, leafHubName string,
		policyID string) (datastructures.HashSet, error)
	GetNonCompliantClustersByLeafHubAndPolicy(ctx context.Context, tableName string, leafHubName string,
		policyID string) (datastructures.HashSet, error)
	InsertPolicyCompliance(ctx context.Context, tableName string, leafHubName string, clusterName string,
		policyID string, errorString string, compliance string, enforcement string, version string) error
	UpdateEnforcementAndResourceVersion(ctx context.Context, tableName string, leafHubName string, policyID string,
		enforcement string, version string) error
	UpdateComplianceRow(ctx context.Context, tableName string, leafHubName string, clusterName string, policyID string,
		compliance string, version string) error
	UpdatePolicyCompliance(ctx context.Context, tableName string, leafHubName string, policyID string,
		compliance string) error
	DeleteComplianceRow(ctx context.Context, tableName string, leafHubName string, clusterName string,
		policyID string) error
	DeleteAllComplianceRows(ctx context.Context, tableName string, leafHubName string, policyID string) error
	InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, tableName string, leafHubName string, policyID string,
		enforcement string, appliedClusters int, nonCompliantClusters int) error
}
