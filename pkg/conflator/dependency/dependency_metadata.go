package dependency

// NewDependencyMetadata creates a new instance of dependency metadata.
func NewDependencyMetadata(dependency *Dependency, dependencyGeneration uint64) *Metadata {
	return &Metadata{dependency, dependencyGeneration}
}

// Metadata abstracts the needed metadata for dependencies management.
type Metadata struct {
	*Dependency
	Generation uint64
}
