package bundle

type CreateBundleFunction func() Bundle

type Bundle interface {
	GetLeafHubName() string
	GetObjects() []interface{}
	GetGeneration() uint64
}
