package dbsyncer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
	"k8s.io/apimachinery/pkg/types"
)

type specDBObj interface {
	GetName() string
	GetUID() types.UID
}

// GenericHandleBundle A generic function to handle bundles.
// if the row doesn't exist then add it.
// if the row exists then update it.
// if the row isn't in the bundle then delete it.
// saves the json file in the DB.
func GenericHandleBundle(ctx context.Context, b bundle.Bundle, scheme string,
	tableName string, dbCLient db.GenericDBTransport, log logr.Logger) error {
	logBundleHandlingMessage(log, b, startBundleHandlingMessage)
	leafHubName := b.GetLeafHubName()

	objectIDsFromDB, err := dbCLient.GetDistinctIDsFromLH(ctx, scheme, tableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s.%s' IDs from dbCLient - %w", scheme, leafHubName, err)
	}

	batchBuilder := dbCLient.NewGenericBatchBuilder(scheme, tableName, leafHubName)

	for _, object := range b.GetObjects() {
		specificObj, ok := object.(specDBObj)
		if !ok {
			continue
		}

		specificObjInd, err := helpers.GetObjectIndex(objectIDsFromDB, string(specificObj.GetUID()))
		if err != nil { // usefulObj not found, new specificObj id
			batchBuilder.Insert(string(specificObj.GetUID()), object)
			// we can continue since its not in objectIDsFromDB anyway
			continue
		}
		// since this already exists in the dbCLient and in the bundle we need to update it
		batchBuilder.Update(string(specificObj.GetUID()), object)
		// we dont want to delete it later
		objectIDsFromDB = append(objectIDsFromDB[:specificObjInd], objectIDsFromDB[specificObjInd+1:]...)
	}

	for _, id := range objectIDsFromDB {
		batchBuilder.Delete(id)
	}

	if err := dbCLient.SendBatch(ctx, batchBuilder.Build()); err != nil {
		return fmt.Errorf("failed to perform batch - %w", err)
	}

	logBundleHandlingMessage(log, b, finishBundleHandlingMessage)

	return nil
}
