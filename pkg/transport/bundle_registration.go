package transport

import (
	"github.com/stolostron/hub-of-hubs-status-transport-bridge/pkg/bundle"
)

// BundleRegistration abstract the registration for bundles according to bundle key in transport layer.
type BundleRegistration struct {
	MsgID            string
	CreateBundleFunc bundle.CreateBundleFunction
	Predicate        func() bool
}
