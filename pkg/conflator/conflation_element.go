package conflator

type conflationElement struct {
	bundleInfo      *BundleInfo
	handlerFunction BundleHandlerFunc
	isInProcess     bool
}
