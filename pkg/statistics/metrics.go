package statistics

// bundleMetrics aggregates metrics per specific bundle type.
type bundleMetrics struct {
	conflationUnit conflationUnitMeasurement // measures a time and conflations while bundle waits in CU's priority queue
	database       timeMeasurement           // measures a time took by db worker to process bundle
}

func newBundleMetrics() *bundleMetrics {
	return &bundleMetrics{conflationUnit: conflationUnitMeasurement{startTimestamps: make(map[string]int64)}}
}
