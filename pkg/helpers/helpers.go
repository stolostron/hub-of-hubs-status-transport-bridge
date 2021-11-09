package helpers

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

// GetBundleType returns the concrete type of a bundle.
func GetBundleType(bundle bundle.Bundle) string {
	array := strings.Split(fmt.Sprintf("%T", bundle), ".")
	return array[len(array)-1]
}

func CreateMapFromRows(rows pgx.Rows, schema, tableName string) (map[string]string, error) {
	result := make(map[string]string)

	for rows.Next() {
		key := "" // the key of the current row.
		val := "" // the value of the current row.

		if err := rows.Scan(&key, &val); err != nil {
			return nil, fmt.Errorf("error reading from table %s.%s - %w", schema, tableName, err)
		}

		result[key] = val
	}

	return result, nil
}
