package db

// DatabaseConnectionPool is the interface needed to work with db workers pool.
type DatabaseConnectionPool interface {
	AcquireConnection() (StatusTransportBridgeDB, error)
	GetPoolSize() int32
	Stop()
}
