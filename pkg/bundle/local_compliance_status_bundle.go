package bundle

// LocalComplianceStatusBundle defined again to avoid bugs.
type LocalComplianceStatusBundle struct{ CompleteComplianceStatusBundle }

// NewLocalComplianceStatusBundle defined again to avoid bugs.
func NewLocalComplianceStatusBundle() *LocalComplianceStatusBundle {
	return &LocalComplianceStatusBundle{}
}
