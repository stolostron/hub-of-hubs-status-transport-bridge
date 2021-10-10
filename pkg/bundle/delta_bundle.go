package bundle

import "errors"

var errWrongType = errors.New("received invalid type")

type DeltaBundle interface {
	Bundle
	InheritContent(olderBundle Bundle) error
}
