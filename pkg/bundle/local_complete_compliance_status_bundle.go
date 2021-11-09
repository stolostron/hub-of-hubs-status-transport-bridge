package bundle

// LocalCompleteComplianceStatusBundle abstracts management of complete compliance status bundle.
type LocalCompleteComplianceStatusBundle struct{ CompleteComplianceStatusBundle }

// NewLocalCompleteComplianceStatusBundle creates a new complete compliance status bundle with no data in it.
func NewLocalCompleteComplianceStatusBundle() Bundle {
	return &LocalCompleteComplianceStatusBundle{}
}
