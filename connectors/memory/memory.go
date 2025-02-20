package memory

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/topology"
)

type Sink[T any] struct {
	ID      string
	Records []T
}

func NewSink[T any](job *topology.Job, id string) *Sink[T] {
	sink := &Sink[T]{
		ID:      id,
		Records: make([]T, 0),
	}
	job.RegisterSink(sink)
	return sink
}

func (s *Sink[T]) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{}
}

func (s *Sink[T]) Collect(ctx context.Context, event T) {
	s.Records = append(s.Records, event)
}
