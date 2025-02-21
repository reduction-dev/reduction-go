package rxn

import (
	"time"
)

// Subject provides APIs for interacting withing the currently keyed scope.
type Subject interface {
	// Timestamp returns the timestamp associated with the current event.
	Timestamp() time.Time
	// Key returns the key associated with the current event.
	Key() []byte
	// SetTimer sets a timer for the current key. After the timer expires, the
	// OnTimerExpired method will be called with this timestamp.
	SetTimer(ts time.Time)
	// Watermark returns the current watermark
	Watermark() time.Time
}
