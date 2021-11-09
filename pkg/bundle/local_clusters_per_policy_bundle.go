package bundle

// LocalClustersPerPolicyBundle abstracts management of clusters per policy bundle.
type LocalClustersPerPolicyBundle struct{ ClustersPerPolicyBundle }

// NewLocalClustersPerPolicyBundle creates a new clusters per policy bundle with no data in it.
func NewLocalClustersPerPolicyBundle() Bundle {
	return &LocalClustersPerPolicyBundle{}
}
