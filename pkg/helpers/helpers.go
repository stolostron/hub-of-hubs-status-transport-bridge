package helpers

import (
	"fmt"
	"strings"

	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

// GetBundleType returns the concrete type of bundle.
func GetBundleType(bundle bundle.Bundle) string {
	array := strings.Split(fmt.Sprintf("%T", bundle), ".")
	return array[len(array)-1]
}

// FormatBundleVersion formats a BundleVersion type into a string of INCARNATION.GENERATION.
func FormatBundleVersion(bv *status.BundleVersion) string {
	return fmt.Sprintf("%d.%d", bv.Incarnation, bv.Generation)
}
