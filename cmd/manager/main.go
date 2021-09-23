package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/controller"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport/kafka"
	hohSyncService "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport/sync-service"
	"github.com/operator-framework/operator-sdk/pkg/log/zap"
	sdkVersion "github.com/operator-framework/operator-sdk/version"
	"github.com/spf13/pflag"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	metricsHost                        = "0.0.0.0"
	metricsPort                  int32 = 9392
	kafkaTransportTypeName             = "kafka"
	syncServiceTransportTypeName       = "syncservice"
	envVarTransportComponent           = "TRANSPORT_TYPE"
	envVarControllerNamespace          = "POD_NAMESPACE"
	leaderElectionLockName             = "hub-of-hubs-status-transport-bridge-lock"
)

var errEnvVarIllegalValue = errors.New("environment variable illegal value")

func printVersion(log logr.Logger) {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))
	log.Info(fmt.Sprintf("Version of operator-sdk: %v", sdkVersion.Version))
}

// function to choose transport type based on env var.
func getTransport(transportType string, conflationMgr *conflator.ConflationManager) (transport.Transport, error) {
	switch transportType {
	case kafkaTransportTypeName:
		kafkaConsumer, err := kafkaclient.NewConsumer(ctrl.Log.WithName("kafka"), conflationMgr)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka-consumer: %w", err)
		}

		return kafkaConsumer, nil

	case syncServiceTransportTypeName:
		syncService, err := hohSyncService.NewSyncService(ctrl.Log.WithName("sync-service"), conflationMgr)
		if err != nil {
			return nil, fmt.Errorf("failed to create sync-service: %w", err)
		}

		return syncService, nil

	default:
		return nil, fmt.Errorf("%w: %s", errEnvVarIllegalValue, transportType)
	}
}

// function to handle defers with exit, see https://stackoverflow.com/a/27629493/553720.
func doMain() int {
	log := initializeLogger()

	leaderElectionNamespace, found := os.LookupEnv(envVarControllerNamespace)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarControllerNamespace)
		return 1
	}

	transportType, found := os.LookupEnv(envVarTransportComponent)
	if !found {
		log.Error(nil, "Not found:", "environment variable", envVarTransportComponent)
		return 1
	}

	// db layer initialization - worker pool + connection pool
	dbWorkerPool, err := startDBWorkerPool()
	if err != nil {
		log.Error(err, "initialization error")
		return 1
	}

	defer dbWorkerPool.Stop()

	mgr, transportObj, err := createManager(leaderElectionNamespace, transportType, metricsHost, metricsPort, dbWorkerPool)
	if err != nil {
		log.Error(err, "Failed to create manager")
		return 1
	}

	if err := transportObj.Start(); err != nil {
		log.Error(err, "transport start failure")
		return 1
	}
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

func startDBWorkerPool() (*workerpool.DBWorkerPool, error) {
	dbWorkerPool, err := workerpool.NewDBWorkerPool()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DBWorkerPool - %w", err)
	}

	if err = dbWorkerPool.Start(); err != nil {
		return nil, fmt.Errorf("failed to start DBWorkerPool - %w", err)
	}

	return dbWorkerPool, nil
}

func createManager(leaderElectionNamespace, transportType, metricsHost string, metricsPort int32,
	workersPool *workerpool.DBWorkerPool) (ctrl.Manager, transport.Transport, error) {
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
	conflationReadyQueue := conflator.NewConflationReadyQueue()
	conflationManager := conflator.NewConflationManager(conflationReadyQueue) // manage all Conflation Units
	// transport layer initialization
	transportObj, err := getTransport(transportType, conflationManager)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize transport: %w", err)
	}

	if err := controller.Setup(mgr, workersPool, conflationManager, conflationReadyQueue, transportObj); err != nil {
		return nil, nil, fmt.Errorf("failed to do initial setup of the manager: %w", err)
	}

	return mgr, transportObj, nil
}

func main() {
	os.Exit(doMain())
}
