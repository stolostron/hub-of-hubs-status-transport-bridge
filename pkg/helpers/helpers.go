package helpers

import (
	"errors"
	"fmt"
	"strings"

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
	array := strings.Split(fmt.Sprintf("%T", bundle), ".")
	return array[len(array)-1]
}

// CompareBundleGenerations checks generations of two bundles and returns:
//
// -1 if bundleL >(gen) bundleR.
//
// 0 if bundleL ==(gen) bundleR.
//
// 1 if bundleL <(gen) bundleR.
func CompareBundleGenerations(bundleL, bundleR bundle.Bundle) int {
	// bundle generation is formatted as INCARNATION.GENERATION (uint64.uint64)
	incarnationL, generationL := bundleL.GetGeneration()
	incarnationR, generationR := bundleR.GetGeneration()

	return CompareIncarnationGenerationPairs(incarnationL, generationL, incarnationR, generationR)
}

// CompareIncarnationGenerationPairs compares pairs of (incarnation, generation) of L and R:
//
// -1 if L > R.
//
// 0 if L == R.
//
// 1 if L < R.
func CompareIncarnationGenerationPairs(incarnationL, generationL, incarnationR, generationR uint64) int {
	if incarnationL < incarnationR {
		return 1
	} else if incarnationL > incarnationR {
		return -1
	}

	// incarnations are equal, check generations
	if generationL < generationR {
		return 1
	} else if generationL > generationR {
		return -1
	}

	// equal
	return 0
}
