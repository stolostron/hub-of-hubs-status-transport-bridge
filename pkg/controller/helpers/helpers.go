package helpers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

// BundleHandlerFunc is a function to handle incoming bundle.
type BundleHandlerFunc func(ctx context.Context, bundle bundle.Bundle) error

var errObjectNotFound = errors.New("object not found")

// HandleBundle generic wrapper function to handle a bundle.
func HandleBundle(ctx context.Context, transport transport.Transport, bundle bundle.Bundle,
	lastBundleGeneration *uint64, handlerFunc BundleHandlerFunc) error {
	bundleGeneration := bundle.GetGeneration()
	if bundleGeneration <= *lastBundleGeneration {
		return nil // handle only if bundle is newer than what we've already handled
	}

	if err := handlerFunc(ctx, bundle); err != nil {
		return err
	}

	// otherwise, bundle was handled successfully.
	*lastBundleGeneration = bundleGeneration

	transport.CommitAsync(bundle)

	return nil
}

// HandleRetry function to handle retries.
func HandleRetry(bundle bundle.Bundle, bundleChan chan bundle.Bundle) {
	time.Sleep(time.Second) // TODO reschedule, should use exponential back off
	bundleChan <- bundle
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
