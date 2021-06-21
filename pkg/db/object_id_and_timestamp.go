package db

import (
	"k8s.io/apimachinery/pkg/types"
	"time"
)

type ObjectIdAndTimestamp struct {
	ObjectId            types.UID
	LastUpdateTimestamp time.Time
}
