package helpers

import (
	"context"
	"fmt"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

// BundleHandlerFunc is a function to handle incoming bundle
type BundleHandlerFunc func(ctx context.Context, bundle bundle.Bundle) error

// HandleBundle generic wrapper function to handle a bundle
func HandleBundle(ctx context.Context, bundle bundle.Bundle, lastBundleGeneration *uint64,
	handlerFunc BundleHandlerFunc) error {
	bundleGeneration := bundle.GetGeneration()
	if bundleGeneration <= *lastBundleGeneration {
		return nil // handle only if bundle is newer than what we've already handled
	}

	if err := handlerFunc(ctx, bundle); err != nil {
		return err
	}

	// otherwise, bundle was handled successfully
	*lastBundleGeneration = bundleGeneration
	return nil
}

// GetObjectIndex return object index if exists, otherwise an error
func GetObjectIndex(slice []string, toBeFound string) (int, error) {
	for i, object := range slice {
		if object == toBeFound {
			return i, nil
		}
	}
	return -1, fmt.Errorf("object %s was not found", toBeFound)
}
