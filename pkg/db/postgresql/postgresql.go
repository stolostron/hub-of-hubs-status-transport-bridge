package postgresql

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"k8s.io/apimachinery/pkg/types"
	"log"
	"os"
	"time"
)

const (
	databaseURLEnvVar = "DATABASE_URL"
)

type PostgreSQL struct {
	conn *pgxpool.Pool
}

func NewPostgreSQL() *PostgreSQL {
	databaseURL := os.Getenv(databaseURLEnvVar)
	if databaseURL == "" {
		log.Fatalf("the expected argument %s is not set in environment variables", databaseURLEnvVar)
	}
	dbConnectionPool, err := pgxpool.Connect(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("unable to connect to db: %s", err)
	}
	return &PostgreSQL{
		conn: dbConnectionPool,
	}
}

func (p *PostgreSQL) Stop() {
	p.conn.Close()
}

func (p *PostgreSQL) GetObjectsByLeafHub(tableName string, leafHubId string) ([]*db.ObjectIdAndTimestamp, error) {
	result := make([]*db.ObjectIdAndTimestamp, 0)
	rows, _ := p.conn.Query(context.Background(),
		`SELECT id,leaf_hub_updated_at FROM status.$1 WHERE leaf_hub_id = $2`, tableName, leafHubId)
	for rows.Next() {
		object := db.ObjectIdAndTimestamp{}
		err := rows.Scan(&object.ObjectId, &object.LastUpdateTimestamp)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, &object)
	}

	return result, nil
}

func (p *PostgreSQL) InsertManagedCluster(tableName string, objId types.UID, leafHubId string, status interface{},
	leafHuhLastUpdate *time.Time) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO status.%s (id,leaf_hub_id,status,error,leaf_hub_updated_at) "+
			"values($1, $2, $3::jsonb, $4, $5)", tableName), objId, leafHubId, status, "none", leafHuhLastUpdate)
	if err != nil {
		return fmt.Errorf("failed to insert into database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdateManagedCluster(tableName string, objId types.UID, leafHubId string, status interface{},
	leafHuhLastUpdate *time.Time) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET status = $1 AND leaf_hub_updated_at = $2 WHERE id = $3 AND
			     leaf_hub_id = $4 `, tableName), status, leafHuhLastUpdate, objId, leafHubId)
	if err != nil {
		return fmt.Errorf("failed to update obj in database: %s", err)
	}

	return nil
}
