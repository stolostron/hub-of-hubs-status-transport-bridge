package bundle

type baseBundle struct {
	LeafHubName string `json:"leafHubName"`
	Generation  uint64 `json:"generation"`
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (baseBundle *baseBundle) GetLeafHubName() string {
	return baseBundle.LeafHubName
}

// GetGeneration returns the bundle generation.
func (baseBundle *baseBundle) GetGeneration() uint64 {
	return baseBundle.Generation
}
