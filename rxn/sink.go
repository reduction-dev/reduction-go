package rxn

import "context"

// Sink is a generic interface for collecting values.
type Sink[T any] interface {
	Collect(ctx context.Context, value T)
}
