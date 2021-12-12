package transport

// GetBundlesMetadataFunc is the function to be called by committers to fetch metadata to commit.
type GetBundlesMetadataFunc func() []BundleMetadata

// BundleMetadata may include metadata that relates to transport - e.g. commit offset.
type BundleMetadata interface {
	MarkAsProcessed()
}

// BaseBundleMetadata wraps the shared data/functionality that the different transport BundleMetadata implementations-
// can be based on.
type BaseBundleMetadata struct {
	Processed bool
}

// MarkAsProcessed function that marks the metadata as processed.
func (metadata *BaseBundleMetadata) MarkAsProcessed() {
	metadata.Processed = true
}
