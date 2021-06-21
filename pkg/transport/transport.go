package transport

import "github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"

type Transport interface {
	Register(msgId string, bundleUpdatesChan chan bundle.Bundle, createBundleFunc bundle.CreateBundleFunction)
}
