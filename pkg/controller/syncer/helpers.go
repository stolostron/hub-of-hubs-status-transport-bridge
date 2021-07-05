package syncer

import (
	"fmt"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

type bundleHandlerFunc func(bundle bundle.Bundle) error

func registerToBundleUpdates(transport transport.Transport,
	registration *BundleRegistration, updateChan chan bundle.Bundle) {
	transport.Register(registration.TransportBundleKey, updateChan, registration.CreateBundleFunc)
}

func handleBundle(bundle bundle.Bundle, lastBundleGeneration *uint64, handlerFunc bundleHandlerFunc) error {
	bundleGeneration := bundle.GetGeneration()
	if bundleGeneration <= *lastBundleGeneration {
		return nil // handle only if bundle is newer than what we've already handled
	}
	if err := handlerFunc(bundle); err != nil {
		return err
	}
	//otherwise, bundle was handled successfully
	*lastBundleGeneration = bundleGeneration
	return nil
}

func getObjectIndex(slice []string, toBeFound string) (int, error) {
	for i, object := range slice {
		if object == toBeFound {
			return i, nil
		}
	}
	return -1, fmt.Errorf("object %s was not found", toBeFound)
}
