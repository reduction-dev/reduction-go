package internal

import "time"

type KeyedEvent struct {
	Key       []byte
	Timestamp time.Time
	Value     []byte
}
