package bundle

import "time"

// CreateBundleFunction is a function that specifies how to create a bundle.
type CreateBundleFunction func() Bundle

// Bundle bundles together a set of k8s objects to be sent to leaf hubs via transport layer.
type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetGeneration() uint64
	SetConflationUnitInsertTime(time.Time)
	GetConflationUnitInsertTime() time.Time
}

// BaseHohBundle is a base bundle for all HoH bundles.
type BaseHohBundle struct {
	conflationUnitInsertTime time.Time
}

// SetConflationUnitInsertTime sets a time the bundle was inserted into conflation unit.
func (bundle *BaseHohBundle) SetConflationUnitInsertTime(time time.Time) {
	bundle.conflationUnitInsertTime = time
}

// GetConflationUnitInsertTime gets a time the bundle was inserted into conflation unit.
func (bundle *BaseHohBundle) GetConflationUnitInsertTime() time.Time {
	return bundle.conflationUnitInsertTime
}

// DependantBundle is a bundle that depends on a different bundle.
// to support bundles dependencies additional function is required - GetDependencyGeneration, in order to start
// processing the dependant bundle only after it's required dependency (with required generation) was processed.
type DependantBundle interface {
	Bundle
	GetDependencyGeneration() uint64
}
