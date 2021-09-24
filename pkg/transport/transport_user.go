package transport

// Consumer abstracts the logic required by a transport user, for the Committer to work.
type Consumer interface {
	// GetBundlesMetadata provides collections of PENDING bundle-metadata and NON-PENDING (processed) bundles-metadata.
	GetBundlesMetadata() []*BundleMetadata
}
