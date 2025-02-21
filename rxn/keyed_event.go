package rxn

import (
	"reduction.dev/reduction-go/internal"
)

// KeyedEvent has a Key used for partitioning data and a timestamp used for
// tracking time. It's value is arbitrary byte data.
type KeyedEvent = internal.KeyedEvent
