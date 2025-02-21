package stdio

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
)

type Sink struct {
	ID string
}

type Event []byte

func NewSink(job *topology.Job, id string) *Sink {
	sink := &Sink{ID: id}
	topology.InternalAccess(job).RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() internal.SinkSynthesis {
	return internal.SinkSynthesis{
		Construct: internal.Construct{
			ID:   s.ID,
			Type: "Sink:Stdio",
			Params: map[string]any{
				"ID": s.ID,
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, event Event) {
	internal.SubjectFromContext(ctx).AddSinkRequest(s.ID, event)
}
