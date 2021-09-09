package postgresql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
)

const (
	envVarDatabaseURL = "DATABASE_URL"
)

var errEnvVarNotFound = errors.New("not found environment variable")

// NewPostgreSQL creates a new instance of PostgreSQL object.
func NewPostgreSQL(ctx context.Context) (*PostgreSQL, error) {
	databaseURL, found := os.LookupEnv(envVarDatabaseURL)
	if !found {
		return nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarDatabaseURL)
	}

	dbConnectionPool, err := pgxpool.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db: %w", err)
	}

	return &PostgreSQL{conn: dbConnectionPool}, nil
}

// PostgreSQL abstracts management of PostgreSQL client.
type PostgreSQL struct {
	conn *pgxpool.Pool
}

// Stop function stops PostgreSQL client.
func (p *PostgreSQL) Stop() {
	p.conn.Close()
}

// GetPoolSize returns the max number of connections.
func (p *PostgreSQL) GetPoolSize() int32 {
	return p.conn.Config().MaxConns
}

// GetManagedClustersByLeafHub returns list of managed clusters by leaf hub name.
func (p *PostgreSQL) GetManagedClustersByLeafHub(ctx context.Context, tableName string,
	leafHubName string) ([]*db.ClusterKeyAndVersion, error) {
	result := make([]*db.ClusterKeyAndVersion, 0)
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT cluster_name,resource_version FROM status.%s WHERE 
			leaf_hub_name=$1`, tableName), leafHubName)

	for rows.Next() {
		object := db.ClusterKeyAndVersion{}
		if err := rows.Scan(&object.ClusterName, &object.ResourceVersion); err != nil {
			return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
		}

		result = append(result, &object)
	}

	return result, nil
}

// InsertManagedCluster inserts managed cluster to the db.
func (p *PostgreSQL) InsertManagedCluster(ctx context.Context, tableName string, clusterName string, leafHubName string,
	payload interface{}, version string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO status.%s (cluster_name,leaf_hub_name,payload,
			resource_version) values($1, $2, $3::jsonb, $4)`, tableName), clusterName, leafHubName, payload,
		version); err != nil {
		return fmt.Errorf("failed to insert into database: %w", err)
	}

	return nil
}

// UpdateManagedCluster updates managed cluster row in the db.
func (p *PostgreSQL) UpdateManagedCluster(ctx context.Context, tableName string, clusterName string, leafHubName string,
	payload interface{}, version string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE status.%s SET payload=$1,resource_version=$2 WHERE 
		cluster_name=$3 AND leaf_hub_name=$4`, tableName), payload, version, clusterName, leafHubName); err != nil {
		return fmt.Errorf("failed to update obj in database: %w", err)
	}

	return nil
}

// DeleteManagedCluster deletes a managed cluster from the db.
func (p *PostgreSQL) DeleteManagedCluster(ctx context.Context, tableName string, clusterName string,
	leafHubName string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`DELETE from status.%s WHERE cluster_name=$1 AND leaf_hub_name=$2`,
		tableName), clusterName, leafHubName); err != nil {
		return fmt.Errorf("failed to delete managed cluster from database: %w", err)
	}

	return nil
}

// ManagedClusterExists checks if a managed clusters exists in the db and returns true or false accordingly.
func (p *PostgreSQL) ManagedClusterExists(ctx context.Context, tableName string, leafHubName string,
	objName string) bool {
	var exists bool
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 from status.%s WHERE leaf_hub_name=$1 AND 
			cluster_name=$2)`, tableName), leafHubName, objName).Scan(&exists); err != nil {
		return false
	}

	return exists
}

// GetPolicyIDsByLeafHub returns policy IDs of a specific leaf hub.
func (p *PostgreSQL) GetPolicyIDsByLeafHub(ctx context.Context, tableName string, leafHubName string) ([]string,
	error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT DISTINCT(policy_id) FROM status.%s WHERE leaf_hub_name=$1`,
		tableName), leafHubName)

	for rows.Next() {
		policyID := ""
		if err := rows.Scan(&policyID); err != nil {
			return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
		}

		result = append(result, policyID)
	}

	return result, nil
}

// GetComplianceClustersByLeafHubAndPolicy returns list of clusters by leaf hub and policy.
func (p *PostgreSQL) GetComplianceClustersByLeafHubAndPolicy(ctx context.Context, tableName string, leafHubName string,
	policyID string) ([]string, error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT cluster_name FROM status.%s WHERE leaf_hub_name=$1 AND 
			policy_id=$2`, tableName), leafHubName, policyID)

	for rows.Next() {
		clusterName := ""
		if err := rows.Scan(&clusterName); err != nil {
			return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
		}

		result = append(result, clusterName)
	}

	return result, nil
}

// GetNonCompliantClustersByLeafHubAndPolicy returns a list of non compliant clusters by leaf hub and policy.
func (p *PostgreSQL) GetNonCompliantClustersByLeafHubAndPolicy(ctx context.Context, tableName string,
	leafHubName string, policyID string) ([]string, error) {
	result := make([]string, 0)
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT cluster_name FROM status.%s WHERE leaf_hub_name=$1 AND 
			policy_id=$2 AND compliance!='compliant'`, tableName),
		leafHubName, policyID)

	for rows.Next() {
		clusterName := ""
		if err := rows.Scan(&clusterName); err != nil {
			return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
		}

		result = append(result, clusterName)
	}

	return result, nil
}

// InsertPolicyCompliance inserts a compliance row to the db.
func (p *PostgreSQL) InsertPolicyCompliance(ctx context.Context, tableName string, policyID string, clusterName string,
	leafHubName string, errorString string, compliance string, enforcement string, version string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO status.%s (policy_id,cluster_name,leaf_hub_name,error,
			compliance,enforcement,resource_version) values($1, $2, $3, $4, $5, $6, $7)`, tableName), policyID,
		clusterName, leafHubName, errorString, compliance, enforcement, version); err != nil {
		return fmt.Errorf("failed to insert into database: %w", err)
	}

	return nil
}

// UpdateEnforcementAndResourceVersion updates enforcement and version by leaf hub and policy.
func (p *PostgreSQL) UpdateEnforcementAndResourceVersion(ctx context.Context, tableName string, policyID string,
	leafHubName string, enforcement string, version string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE status.%s SET enforcement=$1,resource_version=$2 WHERE 
			policy_id=$3 AND leaf_hub_name=$4 AND resource_version<$2`, tableName), enforcement, version, policyID,
		leafHubName); err != nil {
		return fmt.Errorf("failed to update compliance resource_version in database: %w", err)
	}

	return nil
}

// UpdateComplianceRow updates a compliance status row in the db.
func (p *PostgreSQL) UpdateComplianceRow(ctx context.Context, tableName string, policyID string, clusterName string,
	leafHubName string, compliance string, version string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE status.%s SET compliance=$1,resource_version=$2 WHERE 
			policy_id=$3 AND leaf_hub_name=$4 AND cluster_name=$5`, tableName), compliance, version, policyID,
		leafHubName, clusterName); err != nil {
		return fmt.Errorf("failed to update compliance row in database: %w", err)
	}

	return nil
}

// UpdatePolicyCompliance updates policy compliance in the db (to all clusters) by leaf hub and policy.
func (p *PostgreSQL) UpdatePolicyCompliance(ctx context.Context, tableName string, policyID string, leafHubName string,
	compliance string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE status.%s SET compliance=$1 WHERE policy_id=$2 AND 
			leaf_hub_name=$3`, tableName), compliance, policyID, leafHubName); err != nil {
		return fmt.Errorf("failed to update policy compliance in database: %w", err)
	}

	return nil
}

// DeleteComplianceRow delets a compliance row from the db.
func (p *PostgreSQL) DeleteComplianceRow(ctx context.Context, tableName string, policyID string, clusterName string,
	leafHubName string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`DELETE from status.%s WHERE policy_id=$1 AND cluster_name=$2 AND 
			leaf_hub_name=$3`, tableName), policyID, clusterName, leafHubName); err != nil {
		return fmt.Errorf("failed to delete compliance row from database: %w", err)
	}

	return nil
}

// DeleteAllComplianceRows delete all compliance rows from the db by leaf hub and policy.
func (p *PostgreSQL) DeleteAllComplianceRows(ctx context.Context, tableName string, policyID string,
	leafHubName string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`DELETE from status.%s WHERE policy_id=$1 AND leaf_hub_name=$2`,
		tableName), policyID, leafHubName); err != nil {
		return fmt.Errorf("failed to delete compliance rows from database: %w", err)
	}

	return nil
}

// InsertOrUpdateAggregatedPolicyCompliance inserts or updates aggregated policy compliance row in the db.
func (p *PostgreSQL) InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, tableName string, policyID string,
	leafHubName string, enforcement string, appliedClusters int, nonCompliantClusters int) error {
	var exists bool
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 from status.%s WHERE leaf_hub_name=$1 AND 
			policy_id=$2)`, tableName), leafHubName, policyID).Scan(&exists); err != nil {
		return fmt.Errorf("failed to read from database: %w", err)
	}

	if exists { // row for (policy_id,leaf hub) tuple exists, update to the db.
		if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE status.%s SET enforcement=$1,applied_clusters=$2,
			non_compliant_clusters=$3 WHERE policy_id=$4 AND leaf_hub_name=$5`, tableName), enforcement,
			appliedClusters, nonCompliantClusters, policyID, leafHubName); err != nil {
			return fmt.Errorf("failed to update compliance row in database: %w", err)
		}
	} else { // row for (policy_id,leaf hub) tuple doesn't exist, insert to the db.
		if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO status.%s (policy_id,leaf_hub_name,enforcement,
			applied_clusters,non_compliant_clusters) values($1, $2, $3, $4, $5)`, tableName), policyID, leafHubName,
			enforcement, appliedClusters, nonCompliantClusters); err != nil {
			return fmt.Errorf("failed to insert into database: %w", err)
		}
	}

	return nil
}

// UpdateHeartbeat inserts or updates heartbeat for a leaf hub.
func (p *PostgreSQL) UpdateHeartbeat(ctx context.Context, tableName string, leafHubName string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO status.%[1]s (name, last_timestamp) values($1, $2) 
		ON CONFLICT (name) DO UPDATE SET last_timestamp = $2 WHERE status.%[1]s.name = $1`, tableName),
		leafHubName, time.Now().UTC()); err != nil {
		return fmt.Errorf("failed upsert into database: %w", err)
	}

	return nil
}
