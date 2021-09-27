package helpers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

var errObjectNotFound = errors.New("object not found")

// GetBundleType returns the concrete type of a bundle.
func GetBundleType(bundle bundle.Bundle) string {
	array := strings.Split(fmt.Sprintf("%T", bundle), ".")
	return array[len(array)-1]
}

// GetObjectIndex return object index if exists, otherwise an error.
func GetObjectIndex(slice []string, toBeFound string) (int, error) {
	for i, object := range slice {
		if object == toBeFound {
			return i, nil
		}
	}

	return -1, fmt.Errorf("%w - %s", errObjectNotFound, toBeFound)
}
