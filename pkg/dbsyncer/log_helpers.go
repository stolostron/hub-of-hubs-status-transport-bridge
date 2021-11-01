package dbsyncer

import (
	"github.com/go-logr/logr"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/helpers"
)

const (
	startBundleHandlingMessage  = "started handling bundle"
	finishBundleHandlingMessage = "finished handling bundle"
)

func logBundleHandlingMessage(log logr.Logger, bundle bundle.Bundle, message string) {
	log.Info(message, "BundleType", helpers.GetBundleType(bundle), "LeafHubName", bundle.GetLeafHubName(),
		"Version", helpers.FormatBundleVersion(bundle.GetVersion()))
}
