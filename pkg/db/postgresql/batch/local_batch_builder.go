package batch

import (
	"fmt"
	"strings"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	localSpecJsonbColumnIndex = 3
	localSpecUUIDColumnIndex  = 1
	localSpecDeleteRowKey     = "id::text"
)

// NewLocalBatchBuilder creates a new instance of PostgreSQL ManagedClustersBatchBuilder.
func NewLocalBatchBuilder(schema string, tableName string, leafHubName string) *LocalBatchBuilder {
	tableSpecialColumns := make(map[int]string)
	tableSpecialColumns[localSpecJsonbColumnIndex] = db.Jsonb
	tableSpecialColumns[localSpecUUIDColumnIndex] = db.UUID
	builder := &LocalBatchBuilder{
		baseBatchBuilder: newBaseBatchBuilder(schema, tableName, tableSpecialColumns, leafHubName,
			localSpecDeleteRowKey),
	}

	builder.setUpdateStatementFunc(builder.generateUpdateStatement)

	return builder
}

// LocalBatchBuilder is the PostgreSQL implementation of the ManagedClustersBatchBuilder interface.
type LocalBatchBuilder struct {
	*baseBatchBuilder
}

// InsertLocal adds the given (cluster payload, error string) to the batch to be inserted to the db.
func (builder *LocalBatchBuilder) InsertLocal(id string, payload interface{}) {
	builder.insert(id, builder.leafHubName, payload)
}

// UpdateLocal adds the given arguments to the batch to update clusterName with the given payload in db.
func (builder *LocalBatchBuilder) UpdateLocal(id string, payload interface{}) {
	builder.update(id, builder.leafHubName, payload)
}

// DeleteLocal adds delete statement to the batch to delete the given cluster from db.
func (builder *LocalBatchBuilder) DeleteLocal(id string) {
	builder.delete(id)
}

// Build builds the batch object.
func (builder *LocalBatchBuilder) Build() interface{} {
	return builder.build()
}

func (builder *LocalBatchBuilder) generateUpdateStatement() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(fmt.Sprintf("UPDATE %s.%s AS old SET payload=new.payload FROM (values ",
		builder.schema, builder.tableName))

	numberOfColumns := len(builder.updateArgs) / builder.updateRowsCount // total num of args divided by num of rows
	stringBuilder.WriteString(builder.generateInsertOrUpdateArgs(builder.updateRowsCount, numberOfColumns,
		builder.tableSpecialColumns))

	stringBuilder.WriteString(") AS new(id,leaf_hub_name,payload) ")
	stringBuilder.WriteString("WHERE old.leaf_hub_name=new.leaf_hub_name ")
	stringBuilder.WriteString("AND old.id=new.id")

	return stringBuilder.String()
}
