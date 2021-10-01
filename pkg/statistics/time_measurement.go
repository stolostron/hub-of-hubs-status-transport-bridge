package statistics

import (
	"fmt"
	"time"
)

// timeMeasurement contains average and maximum times in milliseconds.
type timeMeasurement struct {
	failures      int64 // number of failures
	successes     int64 // number of successes
	totalDuration int64 // in milliseconds
	maxDuration   int64 // in milliseconds
}

func (tm *timeMeasurement) add(duration time.Duration, err error) {
	if err != nil {
		tm.failures++
		return
	}

	durationMilliseconds := duration.Milliseconds()

	tm.successes++
	tm.totalDuration += durationMilliseconds

	if tm.maxDuration < durationMilliseconds {
		tm.maxDuration = durationMilliseconds
	}
}

func (tm *timeMeasurement) average() float64 {
	if tm.successes == 0 {
		return 0
	}

	return float64(tm.totalDuration / tm.successes)
}

func (tm *timeMeasurement) String() string {
	return tm.toString()
}

func (tm *timeMeasurement) toString() string {
	return fmt.Sprintf("failures=%d, successes=%d, average time=%.2f ms, max time=%d ms",
		tm.failures, tm.successes, tm.average(), tm.maxDuration)
}
