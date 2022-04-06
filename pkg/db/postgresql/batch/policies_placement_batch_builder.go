package batch

import (
	"fmt"
	"strings"

	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	policiesPlacementUUIDColumnIndex  = 2
	policiesPlacementJsonbColumnIndex = 3
	policiesPlacementDeleteRowKey     = "id"
)

// NewPoliciesPlacementBatchBuilder creates a new instance of PostgreSQL PoliciesPlacementBatchBuilder.
func NewPoliciesPlacementBatchBuilder(schema string, tableName string,
	leafHubName string) *PoliciesPlacementBatchBuilder {
	tableSpecialColumns := make(map[int]string)
	tableSpecialColumns[policiesPlacementUUIDColumnIndex] = db.UUID
	tableSpecialColumns[policiesPlacementJsonbColumnIndex] = db.Jsonb

	builder := &PoliciesPlacementBatchBuilder{
		baseBatchBuilder: newBaseBatchBuilder(schema, tableName, tableSpecialColumns, leafHubName,
			policiesPlacementDeleteRowKey),
	}

	builder.setUpdateStatementFunc(builder.generateUpdateStatement)

	return builder
}

// PoliciesPlacementBatchBuilder is the PostgreSQL implementation of the PoliciesPlacementBatchBuilder interface.
type PoliciesPlacementBatchBuilder struct {
	*baseBatchBuilder
}

// Insert adds the given (policyID, placement, resourceVersion) to the batch to be inserted to the db.
func (builder *PoliciesPlacementBatchBuilder) Insert(policyID string, placement interface{}, resourceVersion string) {
	builder.insert(builder.leafHubName, policyID, placement, resourceVersion)
}

// Update adds the given (policyID, placement, resourceVersion) to the batch to be updated to the db.
func (builder *PoliciesPlacementBatchBuilder) Update(policyID string, placement interface{}, resourceVersion string) {
	builder.update(builder.leafHubName, policyID, placement, resourceVersion)
}

// Delete adds delete statement to the batch to delete the given policy from db.
func (builder *PoliciesPlacementBatchBuilder) Delete(policyID string) {
	builder.delete(policyID)
}

// Build builds the batch object.
func (builder *PoliciesPlacementBatchBuilder) Build() interface{} {
	return builder.build()
}

func (builder *PoliciesPlacementBatchBuilder) generateUpdateStatement() string {
	var stringBuilder strings.Builder

	stringBuilder.WriteString(fmt.Sprintf(`UPDATE %s.%s AS old SET placement=new.placement,
		resource_version=new.resource_version FROM (values `, builder.schema, builder.tableName))

	numberOfColumns := len(builder.updateArgs) / builder.updateRowsCount // total num of args divided by num of rows
	stringBuilder.WriteString(builder.generateInsertOrUpdateArgs(builder.updateRowsCount, numberOfColumns,
		builder.tableSpecialColumns))

	stringBuilder.WriteString(") AS new(leaf_hub_name,id,placement,resource_version) ")
	stringBuilder.WriteString("WHERE old.leaf_hub_name=new.leaf_hub_name ")
	stringBuilder.WriteString("AND old.id=new.id")

	return stringBuilder.String()
}
