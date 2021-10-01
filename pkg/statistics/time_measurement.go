package statistics

import (
	"fmt"
	"sync"
	"time"
)

// timeMeasurement contains average and maximum times in milliseconds.
type timeMeasurement struct {
	failures      int64 // number of failures
	successes     int64 // number of successes
	totalDuration int64 // in milliseconds
	maxDuration   int64 // in milliseconds
	mutex         sync.Mutex
}

func (tm *timeMeasurement) String() string {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	return tm.toString()
}

func (tm *timeMeasurement) add(duration time.Duration, err error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

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

func (tm *timeMeasurement) toString() string {
	average := 0.0

	if tm.successes != 0 {
		average = float64(tm.totalDuration / tm.successes)
	}

	return fmt.Sprintf("failures=%d, successes=%d, average time=%.2f ms, max time=%d ms",
		tm.failures, tm.successes, average, tm.maxDuration)
}
