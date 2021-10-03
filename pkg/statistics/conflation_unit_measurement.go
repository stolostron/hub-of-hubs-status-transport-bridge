package statistics

import (
	"fmt"
)

// conflationUnitMeasurement extends timeMeasurement and adds conflation measurements.
type conflationUnitMeasurement struct {
	timeMeasurement
	numOfConflations int64
}

// incrementNumberOfConflations increments number of conflations.
func (cum *conflationUnitMeasurement) incrementNumberOfConflations() {
	cum.mutex.Lock()
	defer cum.mutex.Unlock()

	cum.numOfConflations++
}

// toString is a safe version and must be called in every place where we want to print conflationUnitMeasurement's data.
func (cum *conflationUnitMeasurement) toString() string {
	cum.mutex.Lock()
	defer cum.mutex.Unlock()

	return fmt.Sprintf("%s, num of conflations=%d", cum.timeMeasurement.toStringUnsafe(), cum.numOfConflations)
}
