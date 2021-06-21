package object

import (
	"k8s.io/apimachinery/pkg/types"
	"time"
)

type Object interface {
	GetObjectId() types.UID
	GetObject() interface{}
	GetLeafHubLastUpdateTimestamp() *time.Time
}
