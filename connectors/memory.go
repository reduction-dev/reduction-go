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

func NewMemorySink[T any](job *jobs.Job, id string) *MemorySink[T] {
	sink := &MemorySink[T]{
		ID:      id,
		Records: make([]T, 0),
	}
	job.RegisterSink(sink)
	return sink
}

func (s *MemorySink[T]) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{}
}

func (s *MemorySink[T]) Collect(ctx context.Context, event T) {
	s.Records = append(s.Records, event)
}
