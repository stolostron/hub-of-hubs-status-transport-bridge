package statistics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
)

const logIntervalSeconds = 10

// NewStatistics creates a new instance of Statistics.
func NewStatistics(log logr.Logger) *Statistics {
	statistics := &Statistics{
		log:           log,
		bundleMetrics: make(map[string]*bundleMetrics),
	}

	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ClustersPerPolicyBundle{})] = &bundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ComplianceStatusBundle{})] = &bundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.ManagedClustersStatusBundle{})] = &bundleMetrics{}
	statistics.bundleMetrics[helpers.GetBundleType(&bundle.MinimalComplianceStatusBundle{})] = &bundleMetrics{}

	return statistics
}

// Statistics aggregates different statistics.
type Statistics struct {
	log                      logr.Logger
	numOfAvailableDBWorkers  int
	conflationReadyQueueSize int
	bundleMetrics            map[string]*bundleMetrics
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
	bundleMetrics := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bundleMetrics.transport.add(time, err)
}

// AddConflationUnitMetrics adds conflation unit metrics of the specific bundle type.
func (s *Statistics) AddConflationUnitMetrics(bundle bundle.Bundle, time time.Duration, err error) {
	bundleMetrics := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bundleMetrics.conflationUnit.add(time, err)
}

// IncrementNumberOfConflations increments number of conflation of the specific bundle type.
func (s *Statistics) IncrementNumberOfConflations(bundle bundle.Bundle) {
	bundleMetrics := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bundleMetrics.conflationUnit.numOfConflations++
}

// AddDatabaseMetrics adds database metrics of the specific bundle type.
func (s *Statistics) AddDatabaseMetrics(bundle bundle.Bundle, duration time.Duration, err error) {
	bundleMetrics := s.bundleMetrics[helpers.GetBundleType(bundle)]

	bundleMetrics.database.add(duration, err)
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
	ticker := time.NewTicker(logIntervalSeconds * time.Second)

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return

		case <-ticker.C: // dump statistics
			metrics := ""

			for bundleType, bundleMetrics := range s.bundleMetrics {
				metrics += fmt.Sprintf("[%s, (db process {%s}), (cu {%s})], ",
					bundleType, bundleMetrics.database.String(), bundleMetrics.conflationUnit.String())
			}

			// remove redundant suffix after last metrics
			metrics = strings.TrimSuffix(metrics, ", ")

			s.log.Info("statistics:",
				"conflation ready queue size", s.conflationReadyQueueSize,
				"available db workers", s.numOfAvailableDBWorkers,
				"metrics", metrics)
		}
	}
}
