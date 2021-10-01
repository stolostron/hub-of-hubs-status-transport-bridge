package statistics

import (
	"fmt"
)

// conflationUnitMeasurement extends timeMeasurement and adds conflation measurements.
type conflationUnitMeasurement struct {
	timeMeasurement
	numOfConflations int64
}

func (cum *conflationUnitMeasurement) String() string {
	return fmt.Sprintf("%s, num of conflations=%d", cum.toString(), cum.numOfConflations)
}
