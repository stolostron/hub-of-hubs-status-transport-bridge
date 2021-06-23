package db

type StatusTransportBridgeDB interface {
	GetObjectsByLeafHub(tableName string, leafHubId string) ([]*ObjectIdAndVersion, error)
	InsertManagedCluster(tableName string, objId string, leafHubId string, status interface{}, version string) error
	UpdateManagedCluster(tableName string, objId string, leafHubId string, status interface{}, version string) error
	DeleteManagedCluster(tableName string, objId string, leafHubId string) error
}
