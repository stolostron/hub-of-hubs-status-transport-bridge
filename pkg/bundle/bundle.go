package bundle

import (
	"k8s.io/apimachinery/pkg/types"
	"time"
)

type CreateBundleFunction func() Bundle

type Object interface {
	GetObjectId() types.UID
	GetObject() interface{}
	GetLeafHubLastUpdateTimestamp() *time.Time
}

type Bundle interface {
	GetLeafHubId() string
	GetObjects() []Object
}
