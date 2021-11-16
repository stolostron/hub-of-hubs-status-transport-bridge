package bundle

// NewControlInfoStatusBundle creates a new control info bundle with no data in it.
func NewControlInfoStatusBundle() *ControlInfoStatusBundle {
	return &ControlInfoStatusBundle{}
}

// ControlInfoStatusBundle abstracts management of control info bundle.
type ControlInfoStatusBundle struct {
	LeafHubName string `json:"leafHubName"`
	Generation  uint64 `json:"generation"`
}

// GetLeafHubName returns the leaf hub name that sent the bundle.
func (bundle *ControlInfoStatusBundle) GetLeafHubName() string {
	return bundle.LeafHubName
}

// GetObjects returns the objects in the bundle.
func (bundle *ControlInfoStatusBundle) GetObjects() []interface{} {
	return nil
}

// GetGeneration returns the bundle generation.
func (bundle *ControlInfoStatusBundle) GetGeneration() uint64 {
	return bundle.Generation
}
