package db

// BatchBuilder is an interface to build a batch that can be used to be sent to db.
type BatchBuilder interface {
	// Build builds the batch object.
	Build() interface{}
}

// ManagedClustersBatchBuilder is an interface for building a batch to update managed clusters information in db.
type ManagedClustersBatchBuilder interface {
	BatchBuilder
	// Insert adds the given (cluster payload, error string) to the batch to be inserted to the db.
	Insert(payload interface{}, errorString string)
	// Update adds the given arguments to the batch to update clusterName with the given payload in db.
	Update(clusterName string, payload interface{})
	// Delete adds delete statement to the batch to delete the given cluster from db.
	Delete(clusterName string)
}

// PoliciesBatchBuilder is an interface for building a batch to update policies status information in db.
type PoliciesBatchBuilder interface {
	BatchBuilder
	// Insert adds the given (policyID, clusterName, errorString, compliance) to the batch to be inserted to the db.
	Insert(policyID string, clusterName string, errorString string, compliance ComplianceStatus)
	// Update adds the given (policyID, compliance) to the batch to be updated in the db.
	UpdatePolicyCompliance(policyID string, compliance ComplianceStatus)
	// Update adds the given (policyID, clusterName, compliance) to the batch to be updated in the db.
	UpdateClusterCompliance(policyID string, clusterName string, compliance ComplianceStatus)
	// DeletePolicy adds delete statement to the batch to delete the given policyID from db.
	DeletePolicy(policyID string)
	// DeleteClusterStatus adds delete statement to the batch to delete the given (policyID,clusterName) from db.
	DeleteClusterStatus(policyID string, clusterName string)
}

// GenericBatchBuilder is a generic interface to build a batch and update the db.
type GenericBatchBuilder interface {
	BatchBuilder
	Insert(id string, payload interface{})
	Delete(id string)
	Update(id string, payload interface{})
}
