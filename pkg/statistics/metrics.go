package statistics

// bundleMetrics aggregates metrics per specific bundle type.
type bundleMetrics struct {
	transport      timeMeasurement           // measures a time between bundle send from LH till it was received by HoH
	conflationUnit conflationUnitMeasurement // measures a time and conflations while bundle waits in CU's priority queue
	database       timeMeasurement           // measures a time took by db worker to process bundle
}
