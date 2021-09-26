package dependency

// Type represents type of dependency. possible types are ImplicitDependency or ExplicitDependency.
type Type uint8

const (
	// ImplicitDependency represents an implicit dependency, means it doesn't rely on specific generation.
	ImplicitDependency Type = iota // ImplicitDependency = 0
	// ExplicitDependency represent an explicit dependency, means it relies on a specific bundle generation.
	ExplicitDependency Type = iota // ExplicitDependency = 1
)
