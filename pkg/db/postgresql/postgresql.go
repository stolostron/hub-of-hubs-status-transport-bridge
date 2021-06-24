package postgresql

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"log"
	"os"
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

func (p *PostgreSQL) GetObjectsByLeafHub(tableName string, leafHubName string) ([]*db.ObjectNameAndVersion, error) {
	result := make([]*db.ObjectNameAndVersion, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT cluster_name,resource_version FROM status.%s WHERE leaf_hub_name=$1`, tableName),
		leafHubName)
	for rows.Next() {
		object := db.ObjectNameAndVersion{}
		err := rows.Scan(&object.ObjectName, &object.ResourceVersion)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, &object)
	}

	return result, nil
}

func (p *PostgreSQL) InsertManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
	version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO status.%s (cluster_name,leaf_hub_name,payload,resource_version) 
			values($1, $2, $3::jsonb, $4)`, tableName), objName, leafHubName, payload, version)
	if err != nil {
		return fmt.Errorf("failed to insert into database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdateManagedCluster(tableName string, objName string, leafHubName string, payload interface{},
	version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET payload=$1,resource_version=$2 WHERE cluster_name=$3 AND 
			leaf_hub_name=$4`, tableName), payload, version, objName, leafHubName)

	if err != nil {
		return fmt.Errorf("failed to update obj in database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) DeleteManagedCluster(tableName string, objName string, leafHubName string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`DELETE from status.%s WHERE cluster_name=$1 AND leaf_hub_name=$2`, tableName), objName,
		leafHubName)
	if err != nil {
		return fmt.Errorf("failed to update obj in database: %s", err)
	}

	return nil
}
