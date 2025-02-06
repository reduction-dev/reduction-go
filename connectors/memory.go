package connectors

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type MemorySink[T any] struct {
	ID      string
	Records []T
}

func NewMemorySink[T any](ctx *jobs.JobContext, id string) *MemorySink[T] {
	sink := &MemorySink[T]{
		ID:      id,
		Records: make([]T, 0),
	}
	ctx.RegisterSink(sink)
	return sink
}

func (s *MemorySink[T]) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{}
}

func (s *MemorySink[T]) Runtime(ctx *jobs.OperatorContext) *MemorySinkRuntime[T] {
	runtime := &MemorySinkRuntime[T]{ID: s.ID, Collector: func(event T) {
		s.Records = append(s.Records, event)
	}}
	ctx.RegisterSink(s)
	return runtime
}

type MemorySinkRuntime[T any] struct {
	ID        string
	Collector func(event T)
}

func (s *MemorySinkRuntime[T]) Collect(ctx context.Context, event T) {
	s.Collector(event)
}
