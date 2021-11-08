package batch

import (
	"fmt"
	"strings"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	JsonbColumnIndex = 2
	UUIDColumnIndex  = 1
	DeleteRowKey     = "id::text"
)

// NewGenericBatchBuilder creates a new instance of PostgreSQL ManagedClustersBatchBuilder.
func NewGenericBatchBuilder(schema string, tableName string, leafHubName string) *GenericBatchBuilder {
	tableSpecialColumns := make(map[int]string)
	tableSpecialColumns[JsonbColumnIndex] = db.Jsonb
	tableSpecialColumns[UUIDColumnIndex] = db.UUID
	builder := &GenericBatchBuilder{
		baseBatchBuilder: newBaseBatchBuilder(schema, tableName, tableSpecialColumns, leafHubName,
			DeleteRowKey),
	}

	builder.setUpdateStatementFunc(builder.generateUpdateStatement)

	return builder
}

// GenericBatchBuilder is the PostgreSQL implementation of the ManagedClustersBatchBuilder interface.
type GenericBatchBuilder struct {
	*baseBatchBuilder
}

// Insert adds the given (cluster payload, error string) to the batch to be inserted to the db.
func (builder *GenericBatchBuilder) Insert(id string, payload interface{}) {
	builder.insert(id, payload, builder.leafHubName)
}

// Update adds the given arguments to the batch to update clusterName with the given payload in db.
func (builder *GenericBatchBuilder) Update(id string, payload interface{}) {
	builder.update(id, payload, builder.leafHubName)
}

// Delete adds delete statement to the batch to delete the given cluster from db.
func (builder *GenericBatchBuilder) Delete(id string) {
	builder.delete(id)
}

// Build builds the batch object.
func (builder *GenericBatchBuilder) Build() interface{} {
	return builder.build()
}

func (builder *GenericBatchBuilder) generateUpdateStatement() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(fmt.Sprintf("UPDATE %s.%s AS old SET payload=new.payload FROM (values ",
		builder.schema, builder.tableName))

	numberOfColumns := len(builder.updateArgs) / builder.updateRowsCount // total num of args divided by num of rows
	stringBuilder.WriteString(builder.generateInsertOrUpdateArgs(builder.updateRowsCount, numberOfColumns,
		builder.tableSpecialColumns))

	stringBuilder.WriteString(") AS new(id,payload,leaf_hub_name) ")
	stringBuilder.WriteString("WHERE old.leaf_hub_name=new.leaf_hub_name ")
	stringBuilder.WriteString("AND old.id=new.id")

	return stringBuilder.String()
}
