package rxn

import "context"

type Sink[T any] interface {
	Collect(ctx context.Context, value T)
}
