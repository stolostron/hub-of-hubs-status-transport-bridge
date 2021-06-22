package db

import (
	"time"
)

type ObjectIdAndTimestamp struct {
	ObjectId            string
	LastUpdateTimestamp time.Time
}
