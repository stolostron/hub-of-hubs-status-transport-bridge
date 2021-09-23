package transport

// BundleMetadata wraps the info required for the associated bundle to be used for committing purposes.
type BundleMetadata struct {
	Processed bool
	Partition int32
	Offset    int64
	Topic     string
}
