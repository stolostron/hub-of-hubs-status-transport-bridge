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
	count         int64
	totalDuration time.Duration // in milliseconds
	maxDuration   time.Duration // in milliseconds
}

func (tm *TimeMeasurement) add(duration time.Duration) {
	duration /= time.Millisecond

	tm.count++
	tm.totalDuration += duration

	if tm.maxDuration < duration {
		tm.maxDuration = duration
	}
}

func (tm *TimeMeasurement) average() float64 {
	if tm.count == 0 {
		return 0
	}

	return float64(int64(tm.totalDuration) / tm.count)
}

// BundleMetrics aggregates metrics per specific bundle type.
type BundleMetrics struct {
	transport      TimeMeasurement // measures a time between bundle send from LH till it was received by HoH
	conflationUnit TimeMeasurement // measures a time bundle waits in CU's priority queue
	database       TimeMeasurement // measures a time took by db worker to process bundle
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

// Start starts the dispatcher.
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

// AddTransportMetrics adds transport metrics of the specific bundle type.
func (s *Statistics) AddTransportMetrics(bundle bundle.Bundle, time time.Duration) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.transport.add(time)
}

// AddConflationUnitMetrics adds conflation unit metrics of the specific bundle type.
func (s *Statistics) AddConflationUnitMetrics(bundle bundle.Bundle, time time.Duration) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.conflationUnit.add(time)
}

// AddDatabaseMetrics adds database metrics of the specific bundle type.
func (s *Statistics) AddDatabaseMetrics(bundle bundle.Bundle, duration time.Duration) {
	bm := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bm.database.add(duration)
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
				metrics += "[" + bt + " (db process - "
				metrics += fmt.Sprintf("count=%d", bm.database.count) + ", "
				metrics += fmt.Sprintf("average time=%f ms", bm.database.average()) + ", "
				metrics += fmt.Sprintf("max time=%d ms", bm.database.maxDuration) + ","
				metrics += ")] "
			}

			s.log.Info("statistics:",
				"conflation ready queue size", s.conflationReadyQueueSize,
				"available db workers", s.numOfAvailableDBWorkers,
				"metrics", metrics)
		}
	}
}
