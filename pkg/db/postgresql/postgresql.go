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

func (p *PostgreSQL) GetManagedClustersByLeafHub(tableName string, leafHubName string) ([]*db.ClusterKeyAndVersion,
	error) {
	result := make([]*db.ClusterKeyAndVersion, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT cluster_name,resource_version FROM status.%s WHERE leaf_hub_name=$1`, tableName),
		leafHubName)
	for rows.Next() {
		object := db.ClusterKeyAndVersion{}
		err := rows.Scan(&object.ClusterName, &object.ResourceVersion)
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
		return fmt.Errorf("failed to delete managed cluster from database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) ManagedClusterExists(tableName string, leafHubName string, objName string) bool {
	var exists bool
	err := p.conn.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT EXISTS(SELECT 1 from status.%s WHERE leaf_hub_name=$1 AND cluster_name=$2)`,
			tableName), leafHubName, objName).Scan(&exists)
	if err != nil {
		log.Println(err)
		return false
	}
	return exists
}

func (p *PostgreSQL) GetPolicyIDsByLeafHub(tableName string, leafHubName string) ([]string, error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT DISTINCT(policy_id) FROM status.%s WHERE leaf_hub_name=$1`, tableName), leafHubName)
	for rows.Next() {
		policyId := ""
		err := rows.Scan(&policyId)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, policyId)
	}

	return result, nil
}

func (p *PostgreSQL) GetComplianceClustersByLeafHubAndPolicy(tableName string, leafHubName string,
	policyId string) ([]string, error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT cluster_name FROM status.%s WHERE leaf_hub_name=$1 AND policy_id=$2`, tableName),
		leafHubName, policyId)
	for rows.Next() {
		clusterName := ""
		err := rows.Scan(&clusterName)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, clusterName)
	}

	return result, nil
}

func (p *PostgreSQL) GetNonCompliantClustersByLeafHubAndPolicy(tableName string, leafHubName string,
	policyId string) ([]string, error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT cluster_name FROM status.%s WHERE leaf_hub_name=$1 AND policy_id=$2 AND 
			compliance!='compliant'`, tableName),
		leafHubName, policyId)
	for rows.Next() {
		clusterName := ""
		err := rows.Scan(&clusterName)
		if err != nil {
			log.Printf("error reading from table status.%s - %s", tableName, err)
			return nil, err
		}
		result = append(result, clusterName)
	}

	return result, nil
}

func (p *PostgreSQL) InsertPolicyCompliance(tableName string, policyId string, clusterName string, leafHubName string,
	error string, compliance string, enforcement string, version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`INSERT INTO status.%s (policy_id,cluster_name,leaf_hub_name,error,compliance,enforcement,
			resource_version) values($1, $2, $3, $4, $5, $6, $7)`, tableName), policyId, clusterName, leafHubName,
		error, compliance, enforcement, version)
	if err != nil {
		return fmt.Errorf("failed to insert into database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdateEnforcementAndResourceVersion(tableName string, policyId string, leafHubName string,
	enforcement string, version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET enforcement=$1,resource_version=$2 WHERE policy_id=$3 AND 
			leaf_hub_name=$4 AND resource_version<$2`,
			tableName), enforcement, version, policyId, leafHubName)

	if err != nil {
		return fmt.Errorf("failed to update compliance resource_version in database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdateComplianceRow(tableName string, policyId string, clusterName string, leafHubName string,
	compliance string, version string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET compliance=$1,resource_version=$2 WHERE policy_id=$3 AND 
			leaf_hub_name=$4 AND cluster_name=$5`, tableName), compliance, version, policyId, leafHubName, clusterName)
	if err != nil {
		return fmt.Errorf("failed to update compliance row in database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) UpdatePolicyCompliance(tableName string, policyId string, leafHubName string,
	compliance string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`UPDATE status.%s SET compliance=$1 WHERE policy_id=$2 AND leaf_hub_name=$3`, tableName),
		compliance, policyId, leafHubName)
	if err != nil {
		return fmt.Errorf("failed to update policy compliance in database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) DeleteComplianceRow(tableName string, policyId string, clusterName string,
	leafHubName string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`DELETE from status.%s WHERE policy_id=$1 AND cluster_name=$2 AND leaf_hub_name=$3`,
			tableName), policyId, clusterName, leafHubName)
	if err != nil {
		return fmt.Errorf("failed to delete compliance row from database: %s", err)
	}

	return nil
}

func (p *PostgreSQL) DeleteAllComplianceRows(tableName string, policyId string, leafHubName string) error {
	_, err := p.conn.Exec(context.Background(),
		fmt.Sprintf(`DELETE from status.%s WHERE policy_id=$1 AND leaf_hub_name=$2`, tableName), policyId,
		leafHubName)
	if err != nil {
		return fmt.Errorf("failed to delete compliance rows from database: %s", err)
	}

	return nil
}
