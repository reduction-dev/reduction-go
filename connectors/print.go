package connectors

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

// Sink Buildtime Config

type PrintSink struct {
	ID string
}

type PrintSinkEvent []byte

func NewPrintSink(job *jobs.Job, id string) *PrintSink {
	sink := &PrintSink{ID: id}
	job.RegisterSink(sink)
	return sink
}

func (s *PrintSink) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{
		Construct: types.Construct{
			ID:   s.ID,
			Type: "Sink:Print",
			Params: map[string]any{
				"ID": s.ID,
			},
		},
	}
}

func (s *PrintSink) Collect(ctx context.Context, event PrintSinkEvent) {
	subject, ok := ctx.Value(internal.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}
	subject.AddSinkRequest(s.ID, event)
}
