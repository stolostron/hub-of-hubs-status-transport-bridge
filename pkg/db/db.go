package db

type StatusTransportBridgeDB interface {
	GetObjectsByLeafHub(tableName string, leafHubName string) ([]*ObjectNameAndVersion, error)
	InsertManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	UpdateManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
		version string) error
	DeleteManagedCluster(tableName string, objName string, leafHubName string) error
}
