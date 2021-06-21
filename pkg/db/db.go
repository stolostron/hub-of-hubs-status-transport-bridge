package db

import (
	"time"
)

type StatusTransportBridgeDB interface {
	GetObjectsByLeafHub(tableName string, leafHubId string) ([]*ObjectIdAndTimestamp, error)
	InsertManagedCluster(tableName string, objId string, leafHubId string, status interface{},
		leafHuhLastUpdate *time.Time) error
	UpdateManagedCluster(tableName string, objId string, leafHubId string, status interface{},
		leafHuhLastUpdate *time.Time) error
}
