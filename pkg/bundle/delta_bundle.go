package bundle

import "errors"

var errWrongType = errors.New("received invalid type")

// DeltaBundle abstracts the functionality required from a Bundle to be used as Delta-State bundle.
type DeltaBundle interface {
	DependantBundle
	InheritContent(olderBundle Bundle) error
}
