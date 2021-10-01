package statistics

import (
	"fmt"
)

// conflationUnitMeasurement extends timeMeasurement and adds conflation measurements.
type conflationUnitMeasurement struct {
	timeMeasurement
	numOfConflations int64
}

// IncrementNumberOfConflations increments number of conflations.
func (cum *conflationUnitMeasurement) IncrementNumberOfConflations() {
	cum.mutex.Lock()
	defer cum.mutex.Unlock()

	cum.numOfConflations++
}

func (cum *conflationUnitMeasurement) String() string {
	cum.mutex.Lock()
	defer cum.mutex.Unlock()

	return fmt.Sprintf("%s, num of conflations=%d", cum.toString(), cum.numOfConflations)
}
