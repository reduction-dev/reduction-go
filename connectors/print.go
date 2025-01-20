package connectors

import (
	"context"

	"reduction.dev/reduction-go/rxn"
)

type PrintSink struct {
	ID string
}

type PrintSinkEvent []byte

func (s *PrintSink) Collect(ctx context.Context, event PrintSinkEvent) {
	subject, ok := ctx.Value(rxn.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}
	subject.AddSinkRequest(s.ID, event)
}
