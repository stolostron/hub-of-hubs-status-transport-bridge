package db

import "golang.org/x/net/context"

// StatusTransportBridgeDB is the db interface required by status transport bridge.
type StatusTransportBridgeDB interface {
	ManagedClustersStatusDB
	PoliciesStatusDB
}

// ManagedClustersStatusDB is the db interface required by status transport bridge to manage managed clusters status.
type ManagedClustersStatusDB interface {
	GetManagedClustersByLeafHub(ctx context.Context, tableName string, leafHubName string) ([]*ClusterKeyAndVersion,
		error)
	InsertManagedCluster(ctx context.Context, tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	UpdateManagedCluster(ctx context.Context, tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	DeleteManagedCluster(ctx context.Context, tableName string, objName string, leafHubName string) error
}

// PoliciesStatusDB is the db interface required by status transport bridge to manage policy status.
type PoliciesStatusDB interface {
	ManagedClusterExists(ctx context.Context, tableName string, leafHubName string, objName string) bool
	GetPolicyIDsByLeafHub(ctx context.Context, tableName string, leafHubName string) ([]string, error)
	GetComplianceClustersByLeafHubAndPolicy(ctx context.Context, tableName string, leafHubName string,
		policyID string) ([]string, error)
	GetNonCompliantClustersByLeafHubAndPolicy(ctx context.Context, tableName string, leafHubName string,
		policyID string) ([]string, error)
	InsertPolicyCompliance(ctx context.Context, tableName string, policyID string, clusterName string,
		leafHubName string, errorString string, compliance string, enforcement string, version string) error
	UpdateEnforcementAndResourceVersion(ctx context.Context, tableName string, policyID string, leafHubName string,
		enforcement string, version string) error
	UpdateComplianceRow(ctx context.Context, tableName string, policyID string, clusterName string, leafHubName string,
		compliance string, version string) error
	UpdateComplianceSet(ctx context.Context, tableName string, policyID string, clusterNames []string,
		leafHubName string, compliance string, version string) error
	UpdatePolicyCompliance(ctx context.Context, tableName string, policyID string, leafHubName string,
		compliance string) error
	DeleteComplianceRow(ctx context.Context, tableName string, policyID string, clusterName string,
		leafHubName string) error
	DeleteAllComplianceRows(ctx context.Context, tableName string, policyID string, leafHubName string) error
	InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, tableName string, policyID string, leafHubName string,
		enforcement string, appliedClusters int, nonCompliantClusters int) error
}
