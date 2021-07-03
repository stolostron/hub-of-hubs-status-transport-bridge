package syncer

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"

type BundleRegistration struct {
	TransportBundleKey string
	CreateBundleFunc   bundle.CreateBundleFunction
}
