package db

type StatusTransportBridgeDB interface {
	ManagedClustersStatusDB
	PoliciesStatusDB
}

type ManagedClustersStatusDB interface {
	GetManagedClustersByLeafHub(tableName string, leafHubName string) ([]*ClusterKeyAndVersion, error)
	InsertManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	UpdateManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	DeleteManagedCluster(tableName string, objName string, leafHubName string) error
}

type PoliciesStatusDB interface {
	ManagedClusterExists(tableName string, leafHubName string, objName string) bool
	GetPolicyIDsByLeafHub(tableName string, leafHubName string) ([]string, error)
	GetComplianceClustersByLeafHubAndPolicy(tableName string, leafHubName string, policyId string) ([]string, error)
	GetNonCompliantClustersByLeafHubAndPolicy(tableName string, leafHubName string, policyId string) ([]string, error)
	InsertPolicyCompliance(tableName string, policyId string, clusterName string, leafHubName string,
		version string) error
	UpdateResourceVersion(tableName string, policyId string, leafHubName string, version string) error
	UpdateComplianceRow(tableName string, policyId string, clusterName string, leafHubName string, compliance string,
		enforcement string, version string) error
	//UpdateComplianceRowsWithLowerVersion(tableName string, policyId string, leafHubName string, compliance string,
	//	enforcement string, version string) error
	DeleteComplianceRow(tableName string, policyId string, clusterName string, leafHubName string) error
	DeleteAllComplianceRows(tableName string, policyId string, leafHubName string) error
}
