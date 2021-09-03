package workerpool

import (
	"context"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

func newDBWorker(workerID int32, dbWorkerPool chan chan *DBJob, dbConnPool db.StatusTransportBridgeDB) *dbWorker {
	return &dbWorker{
		workerID:     workerID,
		dbWorkerPool: dbWorkerPool,
		dbConnPool:   dbConnPool,
		jobsQueue:    make(chan *DBJob), // no buffering. when worker is working don't queue more jobs.
	}
}

type dbWorker struct {
	workerID     int32
	dbWorkerPool chan chan *DBJob
	dbConnPool   db.StatusTransportBridgeDB
	jobsQueue    chan *DBJob
}

func (worker *dbWorker) start(ctx context.Context) {
	go func() {
		for {
			// add db worker jobs queue into the dbWorkerPool to mark this dbWorker as available.
			// this is done in each iteration after the dbWorker finished handling a job, for receiving a new job.
			worker.dbWorkerPool <- worker.jobsQueue

			select {
			case <-ctx.Done(): // we have received a signal to stop
				close(worker.jobsQueue)
				return

			case job := <-worker.jobsQueue: // dbWorker received a job request.
				job.HandlerFunc(ctx, worker.dbConnPool)
			}
		}
	}()
}
