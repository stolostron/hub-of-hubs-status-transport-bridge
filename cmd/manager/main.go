package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/go-logr/logr"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/controller"
	workerpool "github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/statistics"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport"
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport/kafka"
	syncservice "github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/transport/sync-service"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                        = "0.0.0.0"
	metricsPort                  int32 = 9392
	envVarControllerNamespace          = "POD_NAMESPACE"
	envVarTransportType                = "TRANSPORT_TYPE"
	kafkaTransportTypeName             = "kafka"
	syncServiceTransportTypeName       = "sync-service"
	leaderElectionLockName             = "hub-of-hubs-status-transport-bridge-lock"
)

var errEnvVarIllegalValue = errors.New("environment variable illegal value")

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
}

// function to choose transport type based on env var.
func getTransport(transportType string, conflationMgr *conflator.ConflationManager,
	statistics *statistics.Statistics) (transport.Transport, error) {
	switch transportType {
	case kafkaTransportTypeName:
		kafkaConsumer, err := kafka.NewConsumer(ctrl.Log.WithName("kafka"), conflationMgr, statistics)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka-consumer: %w", err)
		}

		return kafkaConsumer, nil
	case syncServiceTransportTypeName:
		syncService, err := syncservice.NewSyncService(ctrl.Log.WithName("sync-service"), conflationMgr, statistics)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync-service: %w", err)
		}

		return syncService, nil
	default:
		return nil, fmt.Errorf("%w: %s - %s is not a valid option", errEnvVarIllegalValue, envVarTransportType,
			transportType)
	}
}

// function to determine whether the transport component requires initial-dependencies between bundles to be checked
// (on load). If the returned is false, then we may assume that dependency of the initial bundle of
// each type is met. Otherwise, there are no guarantees and the dependencies must be checked.
func requireInitialDependencyChecks(transportType string) bool {
	switch transportType {
	case kafkaTransportTypeName:
		return false
		// once kafka consumer loads up, it starts reading from the earliest un-processed bundle,
		// as in all bundles that precede the latter have been processed, which include its dependency
		// bundle.

		// the order guarantee also guarantees that if while loading this component, a new bundle is written to a-
		// partition, then surely its dependency was written before it (leaf-hub-status-sync on kafka guarantees).
	case syncServiceTransportTypeName:
		fallthrough
	default:
		return true
	}
}

// function to handle defers with exit, see https://stackoverflow.com/a/27629493/553720.
func doMain() int {
	log := initializeLogger()

	leaderElectionNamespace, found := os.LookupEnv(envVarControllerNamespace)
	if !found {
		log.Error(nil, "Environment variable not found", "environment variable", envVarControllerNamespace)
		return 1
	}

	// create statistics
	stats, err := statistics.NewStatistics(ctrl.Log.WithName("statistics"))
	if err != nil {
		log.Error(err, "initialization error")
		return 1
	}

	transportType, found := os.LookupEnv(envVarTransportType)
	if !found {
		log.Error(nil, "Environment variable not found", "environment variable", envVarTransportType)
		return 1
	}

	// db layer initialization - worker pool + connection pool
	dbWorkerPool, err := startDBWorkerPool(stats)
	if err != nil {
		log.Error(err, "initialization error")
		return 1
	}

	defer dbWorkerPool.Stop()

	mgr, transportObj, err := createManager(leaderElectionNamespace, transportType, dbWorkerPool, stats)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	transportObj.Start()
	defer transportObj.Stop()

	log.Info("Starting the Cmd.")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager exited non-zero")
		return 1
	}

	return 0
}

func initializeLogger() logr.Logger {
	pflag.CommandLine.AddFlagSet(zap.FlagSet())
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	ctrl.SetLogger(zap.Logger())
	log := ctrl.Log.WithName("cmd")

	printVersion(log)

	return log
}

func startDBWorkerPool(statistics *statistics.Statistics) (*workerpool.DBWorkerPool, error) {
	dbWorkerPool, err := workerpool.NewDBWorkerPool(ctrl.Log.WithName("db-worker-pool"), statistics)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DBWorkerPool - %w", err)
	}

	if err = dbWorkerPool.Start(); err != nil {
		return nil, fmt.Errorf("failed to start DBWorkerPool - %w", err)
	}

	return dbWorkerPool, nil
}

func createManager(leaderElectionNamespace string, transportType string, workersPool *workerpool.DBWorkerPool,
	statistics *statistics.Statistics) (ctrl.Manager, transport.Transport, error) {
	options := ctrl.Options{
		MetricsBindAddress:      fmt.Sprintf("%s:%d", metricsHost, metricsPort),
		LeaderElection:          true,
		LeaderElectionID:        leaderElectionLockName,
		LeaderElectionNamespace: leaderElectionNamespace,
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a new manager: %w", err)
	}

	if err = controller.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, nil, fmt.Errorf("failed to add schemes: %w", err)
	}
	// conflationReadyQueue is shared between ConflationManager and dispatcher
	conflationReadyQueue := conflator.NewConflationReadyQueue(statistics)
	requireInitialDependencyChecks := requireInitialDependencyChecks(transportType)
	conflationManager := conflator.NewConflationManager(ctrl.Log.WithName("conflation"), conflationReadyQueue,
		requireInitialDependencyChecks, statistics) // manage all Conflation Units

	// transport layer initialization
	transportObj, err := getTransport(transportType, conflationManager, statistics)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize transport: %w", err)
	}

	if err := controller.Setup(mgr, workersPool, conflationManager, conflationReadyQueue, transportObj,
		statistics); err != nil {
		return nil, nil, fmt.Errorf("failed to do initial setup of the manager: %w", err)
	}

	return mgr, transportObj, nil
}

func main() {
	os.Exit(doMain())
}
