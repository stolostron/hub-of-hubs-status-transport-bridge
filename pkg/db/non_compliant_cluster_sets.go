package db

import set "github.com/deckarep/golang-set"

// NewNonCompliantClusterSets creates a new instnace of NonCompliantClusterSets.
func NewNonCompliantClusterSets() *NonCompliantClusterSets {
	return &NonCompliantClusterSets{
		nonCompliantClusters: set.NewSet(),
		unknownClusters:      set.NewSet(),
	}
}

// NonCompliantClusterSets is a data structure to hold both non compliant clusters set and unknown clusters set.
type NonCompliantClusterSets struct {
	nonCompliantClusters set.Set
	unknownClusters      set.Set
}

// AddNonCompliantCluster adds the given cluster name to the non compliant clusters set.
func (sets *NonCompliantClusterSets) AddNonCompliantCluster(clusterName string) {
	sets.nonCompliantClusters.Add(clusterName)
}

// AddUnknownCluster adds the given cluster name to the unknown clusters set.
func (sets *NonCompliantClusterSets) AddUnknownCluster(clusterName string) {
	sets.unknownClusters.Add(clusterName)
}

// GetNonCompliantClusters returns the non compliant clusters set.
func (sets *NonCompliantClusterSets) GetNonCompliantClusters() set.Set {
	return sets.nonCompliantClusters
}

// GetUnknownClusters returns the unknown clusters set.
func (sets *NonCompliantClusterSets) GetUnknownClusters() set.Set {
	return sets.unknownClusters
}
