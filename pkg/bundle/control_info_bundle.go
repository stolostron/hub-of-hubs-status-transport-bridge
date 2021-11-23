package bundle

// NewControlInfoBundle creates a new control info bundle with no data in it.
func NewControlInfoBundle() Bundle {
	return &ControlInfoBundle{}
}

// ControlInfoBundle abstracts management of control info bundle.
type ControlInfoBundle struct {
	LeafHubName string `json:"leafHubName"`
	Generation  uint64 `json:"generation"`
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *ControlInfoBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *ControlInfoBundle) GetObjects() []interface{} {
	return nil
}

// GetGeneration returns the bundle generation.
func (bundle *ControlInfoBundle) GetGeneration() uint64 {
	return bundle.Generation
}
