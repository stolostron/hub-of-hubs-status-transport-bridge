package db

import (
	"k8s.io/apimachinery/pkg/types"
	"time"
)

type StatusTransportBridgeDB interface {
	GetObjectsByLeafHub(tableName string, leafHubId string) ([]*ObjectIdAndTimestamp, error)
	InsertManagedCluster(tableName string, objId types.UID, leafHubId string, status interface{},
		leafHuhLastUpdate *time.Time) error
	UpdateManagedCluster(tableName string, objId types.UID, leafHubId string, status interface{},
		leafHuhLastUpdate *time.Time) error
}
