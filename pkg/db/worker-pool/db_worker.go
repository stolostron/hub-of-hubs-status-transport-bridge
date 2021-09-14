package workerpool

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

// NewDBWorker creates a new instance of DBWorker.
// jobsQueue is initialized with capacity of 1. this is done in order to make sure dispatcher isn't blocked when calling
// to RunAsync, otherwise it will yield cpu to other go routines.
func NewDBWorker(workerID int32, dbWorkersPool chan *DBWorker, dbConnPool db.StatusTransportBridgeDB) *DBWorker {
	return &DBWorker{
		workerID:      workerID,
		dbWorkersPool: dbWorkersPool,
		dbConnPool:    dbConnPool,
		jobsQueue:     make(chan *DBJob, 1),
	}
}

// DBWorker worker within the DB Worker pool. runs as a goroutine and invokes DBJobs.
type DBWorker struct {
	workerID      int32
	dbWorkersPool chan *DBWorker
	dbConnPool    db.StatusTransportBridgeDB
	jobsQueue     chan *DBJob
}

// RunAsync runs DBJob and reports status to the given CU. once the job processing is finished worker returns to the
// worker pool in order to run more jobs.
func (worker *DBWorker) RunAsync(job *DBJob) {
	worker.jobsQueue <- job
}

func (worker *DBWorker) start(ctx context.Context) {
	go func() {
		for {
			// add worker into the dbWorkerPool to mark this worker as available.
			// this is done in each iteration after the worker finished handling a job (or at startup),
			// for receiving a new job to handle.
			worker.dbWorkersPool <- worker

			select {
			case <-ctx.Done(): // we have received a signal to stop
				close(worker.jobsQueue)
				return

			case job := <-worker.jobsQueue: // DBWorker received a job request.
				err := job.handlerFunc(ctx, job.bundle, worker.dbConnPool) // db connection released to pool when done
				job.conflationUnitResultReporter.ReportResult(job.bundleMetadata, err)
			}
		}
	}()
}
