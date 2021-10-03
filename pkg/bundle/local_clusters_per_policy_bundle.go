package bundle

// LocalClustersPerPolicyBundle defined ClustersPerPolicy again to avoid bugs.
type LocalClustersPerPolicyBundle struct{ ClustersPerPolicyBundle }

// NewLocalClustersPerPolicyBundle defined again to avoid bugs.
func NewLocalClustersPerPolicyBundle() *LocalClustersPerPolicyBundle {
	return &LocalClustersPerPolicyBundle{}
}
