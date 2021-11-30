package batch

import (
	"fmt"
	"strings"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	JsonbColumnIndex = 2
	DeleteRowKey     = "payload->'metadata'->>'uid'"
)

// NewLocalGenericBatchBuilder creates a new instance of PostgreSQL ManagedClustersBatchBuilder.
func NewLocalGenericBatchBuilder(schema string, tableName string, leafHubName string) *LocalGenericBatchBuilder {
	tableSpecialColumns := make(map[int]string)
	tableSpecialColumns[JsonbColumnIndex] = db.Jsonb
	builder := &LocalGenericBatchBuilder{
		baseBatchBuilder: newBaseBatchBuilder(schema, tableName, tableSpecialColumns, leafHubName,
			DeleteRowKey),
	}

	builder.setUpdateStatementFunc(builder.generateUpdateStatement)

	return builder
}

// LocalGenericBatchBuilder is the PostgreSQL implementation of the ManagedClustersBatchBuilder interface.
type LocalGenericBatchBuilder struct {
	*baseBatchBuilder
}

// Insert adds the given (cluster payload, error string) to the batch to be inserted to the db.
func (builder *LocalGenericBatchBuilder) Insert(payload interface{}) {
	builder.insert(builder.leafHubName, payload)
}

// Update adds the given arguments to the batch to update clusterName with the given payload in db.
func (builder *LocalGenericBatchBuilder) Update(payload interface{}) {
	builder.update(builder.leafHubName, payload)
}

// Delete adds delete statement to the batch to delete the given cluster from db.
func (builder *LocalGenericBatchBuilder) Delete(id string) {
	builder.delete(id)
}

// Build builds the batch object.
func (builder *LocalGenericBatchBuilder) Build() interface{} {
	return builder.build()
}

func (builder *LocalGenericBatchBuilder) generateUpdateStatement() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(fmt.Sprintf("UPDATE %s.%s AS old SET payload=new.payload FROM (values ",
		builder.schema, builder.tableName))

	numberOfColumns := len(builder.updateArgs) / builder.updateRowsCount // total num of args divided by num of rows
	stringBuilder.WriteString(builder.generateInsertOrUpdateArgs(builder.updateRowsCount, numberOfColumns,
		builder.tableSpecialColumns))

	stringBuilder.WriteString(") AS new(leaf_hub_name,payload) ")
	stringBuilder.WriteString("WHERE old.leaf_hub_name=new.leaf_hub_name ")
	stringBuilder.WriteString("AND old.payload->'metadata'->>'uid'=new.payload->'metadata'->>'uid'")

	return stringBuilder.String()
}
