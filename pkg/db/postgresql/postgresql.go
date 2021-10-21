package postgresql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	set "github.com/deckarep/golang-set"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/db/postgresql/batch"
)

const (
	envVarDatabaseURL = "DATABASE_URL"
)

var (
	errEnvVarNotFound              = errors.New("not found environment variable")
	errBatchDoesNotMatchPostgreSQL = errors.New("given batch doesn't match postgresql library")
	errBatchFailed                 = errors.New("some of the batch statements failed to execute")
)

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

// SendBatch sends a batch operation to the db and returns list of errors if there were any.
func (p *PostgreSQL) SendBatch(ctx context.Context, batch interface{}) error {
	postgreSQLBatch, ok := batch.(*pgx.Batch)
	if !ok {
		return fmt.Errorf("failed to send batch - %w", errBatchDoesNotMatchPostgreSQL)
	}

	if postgreSQLBatch.Len() == 0 { // no statements in the batch
		return nil // then, there is no error
	}

	batchResult := p.conn.SendBatch(ctx, postgreSQLBatch)
	defer batchResult.Close()

	errorStringBuilder := strings.Builder{}

	for i := 0; i < postgreSQLBatch.Len(); i++ {
		_, err := batchResult.Exec()
		if err != nil {
			errorStringBuilder.WriteString(fmt.Errorf("failed to execute batch statement %w, ", err).Error())
		}
	}

	errorString := errorStringBuilder.String()
	if len(errorString) > 0 {
		return fmt.Errorf("%w - %s", errBatchFailed, errorString)
	}

	return nil
}

// NewManagedClustersBatchBuilder creates a new instance of ManagedClustersBatchBuilder.
func (p *PostgreSQL) NewManagedClustersBatchBuilder(schema string, tableName string,
	leafHubName string) db.ManagedClustersBatchBuilder {
	return batch.NewManagedClustersBatchBuilder(schema, tableName, leafHubName)
}

// GetManagedClustersByLeafHub returns list of managed clusters and for each managed cluster it's resourceVersion.
func (p *PostgreSQL) GetManagedClustersByLeafHub(ctx context.Context, schema string, tableName string,
	leafHubName string) (map[string]string, error) {
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT payload->'metadata'->>'name',
		payload->'metadata'->>'resourceVersion' FROM %s.%s WHERE leaf_hub_name=$1`, schema, tableName), leafHubName)

	result := make(map[string]string)

	for rows.Next() {
		clusterName := ""
		resourceVersion := ""

		if err := rows.Scan(&clusterName, &resourceVersion); err != nil {
			return nil, fmt.Errorf("error reading from table %s.%s - %w", schema, tableName, err)
		}

		result[clusterName] = resourceVersion
	}

	return result, nil
}

// NewPoliciesBatchBuilder creates a new instance of PoliciesBatchBuilder.
func (p *PostgreSQL) NewPoliciesBatchBuilder(schema string, tableName string,
	leafHubName string) db.PoliciesBatchBuilder {
	return batch.NewPoliciesBatchBuilder(schema, tableName, leafHubName)
}

// GetComplianceStatusByLeafHub returns a map of policies, each maps to a set of clusters.
func (p *PostgreSQL) GetComplianceStatusByLeafHub(ctx context.Context, schema string, tableName string,
	leafHubName string) (map[string]*db.PolicyClustersSets, error) {
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT id,cluster_name,compliance FROM %s.%s WHERE 
			leaf_hub_name=$1`, schema, tableName), leafHubName)

	result, err := p.buildComplianceClustersSetsFromRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to get compliance rows - %w", err)
	}

	return result, nil
}

// GetNonCompliantClustersByLeafHub returns a map of policies, each maps to sets of (NonCompliant,Unknown) clusters.
func (p *PostgreSQL) GetNonCompliantClustersByLeafHub(ctx context.Context, schema string, tableName string,
	leafHubName string) (map[string]*db.PolicyClustersSets, error) {
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT id,cluster_name,compliance FROM %s.%s WHERE leaf_hub_name=$1
			 AND compliance!='compliant'`, schema, tableName), leafHubName)

	result, err := p.buildComplianceClustersSetsFromRows(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to get compliance rows - %w", err)
	}

	return result, nil
}

func (p *PostgreSQL) buildComplianceClustersSetsFromRows(rows pgx.Rows) (map[string]*db.PolicyClustersSets, error) {
	result := make(map[string]*db.PolicyClustersSets)

	for rows.Next() {
		var (
			policyID, clusterName string
			complianceStatus      db.ComplianceStatus
		)

		if err := rows.Scan(&policyID, &clusterName, &complianceStatus); err != nil {
			return nil, fmt.Errorf("error in reading compliance table rows - %w", err)
		}

		policyClustersSets, found := result[policyID]
		if !found {
			policyClustersSets = db.NewPolicyClusterSets()
			result[policyID] = policyClustersSets
		}

		policyClustersSets.AddCluster(clusterName, complianceStatus)
	}

	return result, nil
}

// GetPolicyIDsByLeafHub returns policy IDs of a specific leaf hub.
func (p *PostgreSQL) GetPolicyIDsByLeafHub(ctx context.Context, schema string, tableName string,
	leafHubName string) (set.Set, error) {
	rows, _ := p.conn.Query(ctx, fmt.Sprintf(`SELECT DISTINCT(policy_id) FROM %s.%s WHERE leaf_hub_name=$1`,
		schema, tableName), leafHubName)

	result := set.NewSet()

	for rows.Next() {
		policyID := ""
		if err := rows.Scan(&policyID); err != nil {
			return nil, fmt.Errorf("error reading from table status.%s - %w", tableName, err)
		}

		result.Add(policyID)
	}

	return result, nil
}

// InsertOrUpdateAggregatedPolicyCompliance inserts or updates aggregated policy compliance row in the db.
func (p *PostgreSQL) InsertOrUpdateAggregatedPolicyCompliance(ctx context.Context, schema string, tableName string,
	leafHubName string, policyID string, appliedClusters int, nonCompliantClusters int) error {
	var exists bool
	if err := p.conn.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS(SELECT 1 from %s.%s WHERE leaf_hub_name=$1 AND 
			id=$2)`, schema, tableName), leafHubName, policyID).Scan(&exists); err != nil {
		return fmt.Errorf("failed to read from database: %w", err)
	}

	if exists { // row for (policy_id,leaf hub) tuple exists, update to the db.
		if _, err := p.conn.Exec(ctx, fmt.Sprintf(`UPDATE %s.%s SET applied_clusters=$1,non_compliant_clusters=$2
			 WHERE leaf_hub_name=$3 AND id=$4`, schema, tableName), appliedClusters, nonCompliantClusters, leafHubName,
			policyID); err != nil {
			return fmt.Errorf("failed to update compliance row in database: %w", err)
		}
	} else { // row for (policy_id,leaf hub) tuple doesn't exist, insertRowsArgs to the db.
		if _, err := p.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.%s (id,leaf_hub_name,applied_clusters,
			non_compliant_clusters) values($1, $2, $3, $4)`, schema, tableName), policyID, leafHubName,
			appliedClusters, nonCompliantClusters); err != nil {
			return fmt.Errorf("failed to insertRowsArgs into database: %w", err)
		}
	}

	return nil
}

// DeleteAllComplianceRows delete all compliance rows from the db by leaf hub and policy.
func (p *PostgreSQL) DeleteAllComplianceRows(ctx context.Context, schema string, tableName string, leafHubName string,
	policyID string) error {
	if _, err := p.conn.Exec(ctx, fmt.Sprintf(`DELETE from %s.%s WHERE leaf_hub_name=$1 AND policy_id=$2`,
		schema, tableName), leafHubName, policyID); err != nil {
		return fmt.Errorf("failed to delete compliance rows from database: %w", err)
	}

	return nil
}
