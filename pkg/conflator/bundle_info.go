package conflator

import (
	"github.com/open-cluster-management/hub-of-hubs-data-types/bundle/status"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/bundle"
	"github.com/open-cluster-management/hub-of-hubs-status-transport-bridge/pkg/transport"
)

type createBundleInfoFunc func(bundleType string) bundleInfo

type bundleInfo interface {
	getBundle() bundle.Bundle
	getMetadata(toDispatch bool) *BundleMetadata
	updateBundle(bundle bundle.Bundle) error
	updateMetadata(version *status.BundleVersion, transportMetadata transport.BundleMetadata, overwriteObject bool)
	getTransportMetadataToCommit() transport.BundleMetadata
	markAsProcessed(processedMetadata *BundleMetadata)
}

type deltaBundleInfo interface {
	bundleInfo
	handleFailure(failedMetadata *BundleMetadata)
}
