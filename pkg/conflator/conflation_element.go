package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	bundleinfo "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/bundle-info"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/conflator/dependency"
)

type conflationElement struct {
	bundleInfo                 bundleinfo.BundleInfo
	handlerFunction            BundleHandlerFunc
	dependency                 *dependency.Dependency
	isInProcess                bool
	lastProcessedBundleVersion *status.BundleVersion
}
