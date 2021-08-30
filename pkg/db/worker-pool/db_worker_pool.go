package workerpool

import (
	"context"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/postgresql"
)

// NewDBWorkerPool returns a new db workers pool dispatcher.
func NewDBWorkerPool() (*DBWorkerPool, error) {
	ctx, cancelContext := context.WithCancel(context.Background())

	dbConnectionPool, err := postgresql.NewPostgreSQLConnectionPool(ctx)
	if err != nil {
		cancelContext()
		return nil, fmt.Errorf("failed to initialize db worker pool - %w", err)
	}

	return &DBWorkerPool{
		ctx:              ctx,
		cancelContext:    cancelContext,
		dbConnectionPool: dbConnectionPool,
		dbWorkers:        make(chan chan *DBJob, dbConnectionPool.GetPoolSize()),
		jobsQueue:        make(chan *DBJob),
	}, nil
}

// DBWorkerPool pool that registers all db workers and the assigns db jobs to available workers.
type DBWorkerPool struct {
	ctx              context.Context
	cancelContext    context.CancelFunc
	dbConnectionPool db.DatabaseConnectionPool
	dbWorkers        chan chan *DBJob // A pool of workers channels that are registered with the db workers pool
	jobsQueue        chan *DBJob
}

// Start function starts the db workers pool.
func (pool *DBWorkerPool) Start() error {
	var i int32
	// start workers and register them within the pool
	for i = 1; i <= pool.dbConnectionPool.GetPoolSize(); i++ {
		dbConnection, err := pool.dbConnectionPool.AcquireConnection()
		if err != nil {
			return fmt.Errorf("failed to start db worker pool - %w", err)
		}

		worker := newDBWorker(i, pool.dbWorkers, dbConnection)
		// register the db worker within the pool
		worker.start(pool.ctx)
	}

	go pool.dispatch(pool.ctx)

	return nil
}

// Stop function stops the dbWorker queue.
func (pool *DBWorkerPool) Stop() {
	pool.cancelContext()
	pool.dbConnectionPool.Stop()
	close(pool.dbWorkers)
}

// QueueDBJob function to add a db job to the queue.
func (pool *DBWorkerPool) QueueDBJob(job *DBJob) {
	pool.jobsQueue <- job
}

// this function doesn't use go routine because we want to be blocked until a dbWorker is available.
// while all workers are working, whoever tries to push a new job will get blocked as well.
// being blocked while all workers are working will allow conflation mechanism on the other side.
func (pool *DBWorkerPool) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case job := <-pool.jobsQueue:
			// try to obtain a dbWorker job channel that is available.
			// this will block until a dbWorker is idle
			jobChannel := <-pool.dbWorkers

			// dispatch the job to the dbWorker job channel
			jobChannel <- job
		}
	}
}
