package dispatcher

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator"
	workerpool "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/worker-pool"
)

// NewDispatcher creates a new instance of Dispatcher.
func NewDispatcher(log logr.Logger, conflationReadyQueue *conflator.ConflationReadyQueue,
	dbWorkerPool *workerpool.DBWorkerPool) *Dispatcher {
	return &Dispatcher{
		log:                    log,
		conflationReadyQueue:   conflationReadyQueue,
		dbWorkerPool:           dbWorkerPool,
		bundleHandlerFunctions: make(map[string]conflator.BundleHandlerFunc),
	}
}

// Dispatcher abstracts the dispatching of db jobs to db workers. this is done by reading ready CU and getting from them
// a ready to process bundles.
type Dispatcher struct {
	log                    logr.Logger
	conflationReadyQueue   *conflator.ConflationReadyQueue
	dbWorkerPool           *workerpool.DBWorkerPool
	bundleHandlerFunctions map[string]conflator.BundleHandlerFunc // maps bundle type to handler function
}

// Start starts the dispatcher.
func (dispatcher *Dispatcher) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	go dispatcher.dispatch(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel
		cancelContext()
		dispatcher.log.Info("stopped dispatcher")

		return nil
	}
}

func (dispatcher *Dispatcher) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // if dispatcher was stopped do not process more bundles
			return

		default: // as long as context wasn't cancelled, continue and try to read bundles to process
			conflationUnit := dispatcher.conflationReadyQueue.BlockingDequeue() // blocking if no CU has ready bundle
			dbWorker := dispatcher.dbWorkerPool.Acquire()                       // blocking if no worker available

			bundle, bundleMetadata, handlerFunction, err := conflationUnit.GetNext()
			if err != nil {
				dispatcher.log.Error(err, "failed to get next bundle")
				continue
			}

			dbWorker.RunAsync(workerpool.NewDBJob(bundle, bundleMetadata, handlerFunction, conflationUnit))
		}
	}
}
