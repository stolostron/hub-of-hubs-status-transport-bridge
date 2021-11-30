package dbsyncer

import (
	"context"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// localGenericHandleBundle A generic function to handle bundles.
// if the row doesn't exist then add it.
// if the row exists then update it.
// if the row isn't in the bundle then delete it.
// saves the json file in the DB.
func localGenericHandleBundle(ctx context.Context, bundle bundle.Bundle, schema string,
	tableName string, dbClient db.LocalPoliciesStatusDB) error {
	leafHubName := bundle.GetLeafHubName()

	idToVersionMapFromDB, err := dbClient.GetDistinctIDAndVersion(ctx, schema, tableName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed fetching leaf hub '%s.%s' IDs from db - %w", schema, tableName, err)
	}

	batchBuilder := dbClient.NewLocalGenericBatchBuilder(schema, tableName, leafHubName)

	for _, object := range bundle.GetObjects() {
		specificObj, ok := object.(metav1.Object)
		if !ok {
			continue
		}

		uid := string(specificObj.GetUID())
		resourceVersionFromDB, objInDB := idToVersionMapFromDB[uid]

		if !objInDB {
			batchBuilder.Insert(object)
			continue
		}

		delete(idToVersionMapFromDB, uid)

		if specificObj.GetResourceVersion() == resourceVersionFromDB {
			continue // update cluster in db only if what we got is a different (newer) version of the resource.
		}

		batchBuilder.Update(object)
	}

	// delete everything that's left.
	for uid := range idToVersionMapFromDB {
		batchBuilder.Delete(uid)
	}

	if err := dbClient.SendBatch(ctx, batchBuilder.Build()); err != nil {
		return fmt.Errorf("failed to perform batch - %w", err)
	}

	return nil
}
