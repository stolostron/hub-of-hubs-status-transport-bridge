package bundle

import (
	clusterv1 "github.com/open-cluster-management/api/cluster/v1"
	"k8s.io/apimachinery/pkg/types"
	"time"
)

type TimestampedManagedCluster struct {
	ManagedCluster      *clusterv1.ManagedCluster `json:"object"`
	LastUpdateTimestamp *time.Time                `json:"lastUpdateTimestamp"`
}

func (o *TimestampedManagedCluster) GetObjectId() types.UID {
	return o.ManagedCluster.UID
}

func (o *TimestampedManagedCluster) GetObject() interface{} {
	return o.ManagedCluster
}

func (o *TimestampedManagedCluster) GetLeafHubLastUpdateTimestamp() *time.Time {
	return o.LastUpdateTimestamp
}
