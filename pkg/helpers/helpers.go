package helpers

import (
	"fmt"
	"strings"

	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

// GetBundleType returns the concrete type of a bundle.
func GetBundleType(bundle bundle.Bundle) string {
	array := strings.Split(fmt.Sprintf("%T", bundle), ".")
	return array[len(array)-1]
}
