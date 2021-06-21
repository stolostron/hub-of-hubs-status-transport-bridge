package controller

import (
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"sync"
)

const (
	managedClustersTableName = "managed_clusters"
)

type StatusTransportBridge struct {
	transportToDBSyncers []*genericTransportToDBSyncer
	stopChan             chan struct{}
	startOnce            sync.Once
	stopOnce             sync.Once
}

func NewStatusTransportBridge(db db.StatusTransportBridgeDB, transport transport.Transport) *StatusTransportBridge {
	stopChan := make(chan struct{}, 1)
	return &StatusTransportBridge{
		stopChan: stopChan,
		transportToDBSyncers: []*genericTransportToDBSyncer{
			{ // syncer for managed clusters
				db:                 db,
				transport:          transport,
				dbTableName:        managedClustersTableName,
				transportBundleKey: datatypes.ManagedClustersMsgKey,
				createBundleFunc:   func() bundle.Bundle { return bundle.NewManagedClustersStatusBundle() },
				stopChan:           stopChan,
			},
		},
	}
}

func (b *StatusTransportBridge) Start() {
	b.startOnce.Do(func() {
		for _, syncer := range b.transportToDBSyncers {
			syncer.Start()
		}
	})
}

func (b *StatusTransportBridge) Stop() {
	b.stopOnce.Do(func() {
		b.stopChan <- struct{}{}
		close(b.stopChan)
	})
}
