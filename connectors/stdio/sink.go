package stdio

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

type Sink struct {
	ID string
}

type Event []byte

func NewSink(job *jobs.Job, id string) *Sink {
	sink := &Sink{ID: id}
	job.RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{
		Construct: types.Construct{
			ID:   s.ID,
			Type: "Sink:Stdio",
			Params: map[string]any{
				"ID": s.ID,
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, event Event) {
	subject, ok := ctx.Value(internal.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}
	subject.AddSinkRequest(s.ID, event)
}
