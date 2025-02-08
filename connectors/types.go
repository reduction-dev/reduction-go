package connectors

import "context"

type SinkRuntime[T any] interface {
	Collect(ctx context.Context, value T)
}
