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

func NewPrintSink(ctx *jobs.JobContext, id string) *PrintSink {
	sink := &PrintSink{ID: id}
	ctx.RegisterSink(sink)
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

func (s *PrintSink) Runtime(ctx *types.OperatorContext) *PrintSinkRuntime {
	sink := &PrintSinkRuntime{ID: s.ID}
	ctx.RegisterSink(s)
	return sink
}

type PrintSinkRuntime struct {
	ID string
}

type PrintSinkEvent []byte

func (s *PrintSinkRuntime) Collect(ctx context.Context, event PrintSinkEvent) {
	subject, ok := ctx.Value(internal.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}
	subject.AddSinkRequest(s.ID, event)
}
