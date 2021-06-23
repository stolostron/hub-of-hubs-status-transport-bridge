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

func (p *PostgreSQL) GetObjectsByLeafHub(tableName string, leafHubId string) ([]*db.ObjectIdAndVersion, error) {
	result := make([]*db.ObjectIdAndVersion, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT id,resource_version FROM status.%s WHERE leaf_hub_id='%s'`, tableName, leafHubId))
	for rows.Next() {
		object := db.ObjectIdAndVersion{}
		err := rows.Scan(&object.ObjectId, &object.ResourceVersion)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, &object)
	}

	return result, nil
}

func (p *PostgreSQL) InsertManagedCluster(tableName string, objId string, leafHubId string, status interface{},
	version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf("INSERT INTO status.%s (id,leaf_hub_id,status,resource_version) "+
			"values($1, $2, $3::jsonb, $4)", tableName), objId, leafHubId, status, version)
	if err != nil {
		return fmt.Errorf("failed to insert into database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdateManagedCluster(tableName string, objId string, leafHubId string, status interface{},
	version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET status=$1,resource_version=$2 WHERE id=$3 AND leaf_hub_id=$4`,
			tableName), status, version, objId, leafHubId)

	if err != nil {
		return fmt.Errorf("failed to update obj in database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) DeleteManagedCluster(tableName string, objId string, leafHubId string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`DELETE from status.%s WHERE id=$1 AND leaf_hub_id=$2`, tableName), objId, leafHubId)
	if err != nil {
		return fmt.Errorf("failed to update obj in database: %s", err)
	}

	return nil
}
