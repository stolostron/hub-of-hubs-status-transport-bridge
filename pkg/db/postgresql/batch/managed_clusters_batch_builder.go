package batch

import (
	"fmt"
	"strings"

	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	managedClustersJsonbColumnIndex = 2
	managedClustersDeleteRowKey     = "payload->'metadata'->>'name'"
)

// NewManagedClustersBatchBuilder creates a new instance of PostgreSQL ManagedClustersBatchBuilder.
func NewManagedClustersBatchBuilder(schema string, tableName string, leafHubName string) *ManagedClustersBatchBuilder {
	tableSpecialColumns := make(map[int]string)
	tableSpecialColumns[managedClustersJsonbColumnIndex] = db.Jsonb

	builder := &ManagedClustersBatchBuilder{
		baseBatchBuilder: newBaseBatchBuilder(schema, tableName, tableSpecialColumns, leafHubName,
			managedClustersDeleteRowKey),
	}

	builder.setUpdateStatementFunc(builder.generateUpdateStatement)

	return builder
}

// ManagedClustersBatchBuilder is the PostgreSQL implementation of the ManagedClustersBatchBuilder interface.
type ManagedClustersBatchBuilder struct {
	*baseBatchBuilder
}

// Insert adds the given (cluster payload, error string) to the batch to be inserted to the db.
func (builder *ManagedClustersBatchBuilder) Insert(payload interface{}, errorString string) {
	builder.insert(builder.leafHubName, payload, errorString)
}

// Update adds the given arguments to the batch to update clusterName with the given payload in db.
func (builder *ManagedClustersBatchBuilder) Update(clusterName string, payload interface{}) {
	builder.update(builder.leafHubName, payload, clusterName)
}

// Delete adds delete statement to the batch to delete the given cluster from db.
func (builder *ManagedClustersBatchBuilder) Delete(clusterName string) {
	builder.delete(clusterName)
}

// Build builds the batch object.
func (builder *ManagedClustersBatchBuilder) Build() interface{} {
	return builder.build()
}

func (builder *ManagedClustersBatchBuilder) generateUpdateStatement() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(fmt.Sprintf("UPDATE %s.%s AS old SET payload=new.payload FROM (values ",
		builder.schema, builder.tableName))

	numberOfColumns := len(builder.updateArgs) / builder.updateRowsCount // total num of args divided by num of rows
	stringBuilder.WriteString(builder.generateInsertOrUpdateArgs(builder.updateRowsCount, numberOfColumns,
		builder.tableSpecialColumns))

	stringBuilder.WriteString(") AS new(leaf_hub_name,payload,cluster_name) ")
	stringBuilder.WriteString("WHERE old.leaf_hub_name=new.leaf_hub_name ")
	stringBuilder.WriteString("AND old.payload->'metadata'->>'name'=new.cluster_name")

	return stringBuilder.String()
}
