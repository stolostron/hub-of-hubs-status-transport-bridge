package bundle

// LocalClustersPerPolicyBundle abstracts management of local clusters per policy bundle.
type LocalClustersPerPolicyBundle struct{ ClustersPerPolicyBundle }

// NewLocalClustersPerPolicyBundle creates a local new clusters per policy bundle with no data in it.
func NewLocalClustersPerPolicyBundle() Bundle {
	return &LocalClustersPerPolicyBundle{}
}
