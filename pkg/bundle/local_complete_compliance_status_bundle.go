package bundle

// LocalCompleteComplianceStatusBundle abstracts management of local complete compliance status bundle.
type LocalCompleteComplianceStatusBundle struct{ CompleteComplianceStatusBundle }

// NewLocalCompleteComplianceStatusBundle creates a new local complete compliance status bundle with no data in it.
func NewLocalCompleteComplianceStatusBundle() Bundle {
	return &LocalCompleteComplianceStatusBundle{}
}
