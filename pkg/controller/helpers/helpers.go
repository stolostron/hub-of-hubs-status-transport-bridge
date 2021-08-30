package helpers

import (
	"errors"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

var errObjectNotFound = errors.New("object not found")

// GetObjectIndex return object index if exists, otherwise an error.
func GetObjectIndex(slice []string, toBeFound string) (int, error) {
	for i, object := range slice {
		if object == toBeFound {
			return i, nil
		}
	}

	return -1, fmt.Errorf("%w - %s", errObjectNotFound, toBeFound)
}

// GetBundleType returns the concrete type of a bundle.
func GetBundleType(bundle bundle.Bundle) string {
	return fmt.Sprintf("%T", bundle)
}
