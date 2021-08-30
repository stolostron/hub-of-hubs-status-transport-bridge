package postgresql

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	envVarDatabaseURL = "DATABASE_URL"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// NewPostgreSQLConnectionPool creates a new instance of ConnectionPool object.
func NewPostgreSQLConnectionPool(ctx context.Context) (*ConnectionPool, error) {
	databaseURL, found := os.LookupEnv(envVarDatabaseURL)
	if !found {
		return nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarDatabaseURL)
	}

	dbConnectionPool, err := pgxpool.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db: %w", err)
	}

	return &ConnectionPool{ctx: ctx, conn: dbConnectionPool}, nil
}

// ConnectionPool abstracts PostgreSQL connection pool.
type ConnectionPool struct {
	ctx  context.Context
	conn *pgxpool.Pool
}

// GetPoolSize returns the max number of connections.
func (pool *ConnectionPool) GetPoolSize() int32 {
	return pool.conn.Config().MaxConns
}

// AcquireConnection acquires a single connection from the connection pool.
func (pool *ConnectionPool) AcquireConnection() (db.StatusTransportBridgeDB, error) {
	conn, err := pool.conn.Acquire(pool.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire postrgre sql connection - %w", err)
	}

	return NewPostgreSQLConnection(conn), nil
}

// Stop function stops PostgreSQL connection pool.
func (pool *ConnectionPool) Stop() {
	pool.conn.Close()
}
