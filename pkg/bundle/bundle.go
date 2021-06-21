package bundle

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle/object"

type CreateBundleFunction func() Bundle

type Bundle interface {
	GetLeafHubId() string
	GetObjects() []object.Object
}
