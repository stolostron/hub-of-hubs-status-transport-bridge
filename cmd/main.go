package main

import (
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/postgresql"
	syncService "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport/sync-service"
)

func main() {
	// db layer initialization
	postgreSQL := postgresql.NewPostgreSQL()
	defer postgreSQL.Stop()

	// transport layer initialization
	syncServiceObj := syncService.NewSyncService()
	syncServiceObj.Start()
	defer syncServiceObj.Stop()

	transportBridgeController := controller.NewStatusTransportBridge(postgreSQL, syncServiceObj)
	transportBridgeController.Start()
	defer transportBridgeController.Stop()
}
