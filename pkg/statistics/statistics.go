package statistics

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
)

const dumpIntervalSeconds = 10

// NewStatistics creates a new instance of Statistics.
func NewStatistics(log logr.Logger) *Statistics {
	statistics := &Statistics{
		log:           log,
		bundleMetrics: make(map[string]*BundleMetrics),
	}

	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ClustersPerPolicyBundle{})] = &BundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ComplianceStatusBundle{})] = &BundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ManagedClustersStatusBundle{})] = &BundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.MinimalComplianceStatusBundle{})] = &BundleMetrics{}

	return statistics
}

// TimeMeasurement contains average and maximum times in milliseconds.
type TimeMeasurement struct {
	failures      int64 // number of failures
	successes     int64 // number of successes
	totalDuration int64 // in milliseconds
	maxDuration   int64 // in milliseconds
}

func (tm *TimeMeasurement) add(duration time.Duration, err error) {
	if err == nil {
		durationMilliseconds := duration.Milliseconds()

		tm.successes++
		tm.totalDuration += durationMilliseconds

		if tm.maxDuration < durationMilliseconds {
			tm.maxDuration = durationMilliseconds
		}
	} else {
		tm.failures++
	}
}

func (tm *TimeMeasurement) average() float64 {
	if tm.successes == 0 {
		return 0
	}

	return float64(tm.totalDuration / tm.successes)
}

func (tm *TimeMeasurement) String() string {
	return "[" + tm.toString() + "]"
}

func (tm *TimeMeasurement) toString() string {
	return fmt.Sprintf("failures=%d, successes=%d, average time=%.2f ms, max time=%d ms",
		tm.failures, tm.successes, tm.average(), tm.maxDuration)
}

// ConflationUnitMeasurement extends TimeMeasurement and adds conflation measurements.
type ConflationUnitMeasurement struct {
	TimeMeasurement
	numOfConflations int64
}

func (cum *ConflationUnitMeasurement) String() string {
	return fmt.Sprintf("[%s, num of conflations=%d]", cum.toString(), cum.numOfConflations)
}

// BundleMetrics aggregates metrics per specific bundle type.
type BundleMetrics struct {
	transport      TimeMeasurement           // measures a time between bundle send from LH till it was received by HoH
	conflationUnit ConflationUnitMeasurement // measures a time and conflations while bundle waits in CU's priority queue
	database       TimeMeasurement           // measures a time took by db worker to process bundle
}

// Statistics aggregates different statistics.
type Statistics struct {
	log                      logr.Logger
	numOfAvailableDBWorkers  int
	conflationReadyQueueSize int
	bundleMetrics            map[string]*BundleMetrics
}

// SetNumberOfAvailableDBWorkers sets number of available db workers.
func (s *Statistics) SetNumberOfAvailableDBWorkers(numOf int) {
	s.numOfAvailableDBWorkers = numOf
}

// SetConflationReadyQueueSize sets conflation ready queue size.
func (s *Statistics) SetConflationReadyQueueSize(size int) {
	s.conflationReadyQueueSize = size
}

// AddTransportMetrics adds transport metrics of the specific bundle type.
func (s *Statistics) AddTransportMetrics(bundle bundle.Bundle, time time.Duration, err error) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.transport.add(time, err)
}

// AddConflationUnitMetrics adds conflation unit metrics of the specific bundle type.
func (s *Statistics) AddConflationUnitMetrics(bundle bundle.Bundle, time time.Duration, err error) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.conflationUnit.add(time, err)
}

// IncrementNumberOfConflations increments number of conflation of the specific bundle type.
func (s *Statistics) IncrementNumberOfConflations(bundle bundle.Bundle) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.conflationUnit.numOfConflations++
}

// AddDatabaseMetrics adds database metrics of the specific bundle type.
func (s *Statistics) AddDatabaseMetrics(bundle bundle.Bundle, duration time.Duration, err error) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.database.add(duration, err)
}

// Start starts the statistics.
func (s *Statistics) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	s.log.Info("started statistics")

	go s.run(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel
		cancelContext()
		s.log.Info("stopped statistics")

		return nil
	}
}

func (s *Statistics) run(ctx context.Context) {
	ticker := time.NewTicker(dumpIntervalSeconds * time.Second)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return

		case <-ticker.C: // dump statistics
			metrics := ""

			for bt, bm := range s.bundleMetrics {
				metrics += "[" + bt
				metrics += " (db process " + bm.database.String() + "),"
				metrics += " (cu " + bm.conflationUnit.String() + ")], "
			}

			metrics = metrics[:len(metrics)-2]

			s.log.Info("statistics:",
				"conflation ready queue size", s.conflationReadyQueueSize,
				"available db workers", s.numOfAvailableDBWorkers,
				"metrics", metrics)
		}
	}
}