package dependency

// NewDependency creates a new instance of dependency.
func NewDependency(bundleType string) *Dependency {
	return &Dependency{
		BundleType: bundleType,
	}
}

// Dependency represents the dependency between different bundles. a bundle can depend only on one other bundle.
type Dependency struct {
	BundleType string
}
