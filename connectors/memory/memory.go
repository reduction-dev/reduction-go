package memory

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
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
	topology.InternalAccess(job).RegisterSink(sink)
	return sink
}

func (s *Sink[T]) Synthesize() internal.SinkSynthesis {
	return internal.SinkSynthesis{
		Construct: internal.Construct{
			ID:   s.ID,
			Type: "Sink:Memory",
			Params: map[string]any{
				"ID": s.ID,
			},
		},
		Config: &jobconfigpb.Sink{
			Id: s.ID,
			Config: &jobconfigpb.Sink_Memory{
				Memory: &jobconfigpb.MemorySink{},
			},
		},
	}
}

func (s *Sink[T]) Collect(ctx context.Context, event T) {
	s.Records = append(s.Records, event)
}
