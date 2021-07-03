package controller

import (
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller/syncer"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	"sync"
)

const (
	managedClustersTableName = "managed_clusters"
	complianceTableName      = "compliance"
)

type StatusTransportBridge struct {
	transportToDBSyncers []syncer.Syncer
	stopChan             chan struct{}
	startOnce            sync.Once
	stopOnce             sync.Once
}

func NewStatusTransportBridge(db db.StatusTransportBridgeDB, transport transport.Transport) *StatusTransportBridge {
	stopChan := make(chan struct{}, 1)
	return &StatusTransportBridge{
		stopChan: stopChan,
		transportToDBSyncers: []syncer.Syncer{
			syncer.NewClustersTransportToDBSyncer( // syncer for managed clusters
				db, transport, managedClustersTableName, &syncer.BundleRegistration{
					TransportBundleKey: datatypes.ManagedClustersMsgKey,
					CreateBundleFunc:   func() bundle.Bundle { return bundle.NewManagedClustersStatusBundle() },
				},
				stopChan),
			syncer.NewPoliciesTransportToDBSyncer( // syncer for policies
				db, transport, managedClustersTableName, complianceTableName, &syncer.BundleRegistration{
					TransportBundleKey: datatypes.ClustersPerPolicyMsgKey,
					CreateBundleFunc:   func() bundle.Bundle { return bundle.NewClustersPerPolicyBundle() },
				}, &syncer.BundleRegistration{
					TransportBundleKey: datatypes.PolicyComplianceMsgKey,
					CreateBundleFunc:   func() bundle.Bundle { return bundle.NewComplianceStatusBundle() },
				},
				stopChan),
		},
	}
}

func (b *StatusTransportBridge) Start() {
	b.startOnce.Do(func() {
		for _, transportToDBSyncer := range b.transportToDBSyncers {
			transportToDBSyncer.StartSync()
		}
		for {
			select { // make sure the program doesn't exist
			case <-b.stopChan:
				return
			}
		}
	})
}

func (b *StatusTransportBridge) Stop() {
	b.stopOnce.Do(func() {
		b.stopChan <- struct{}{}
		close(b.stopChan)
	})
}
