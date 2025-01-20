package jobs

import (
	"context"

	"reduction.dev/reduction-go/rxn"
)

type PrintSink struct {
	ID string
}

func NewPrintSink(id string) *PrintSink {
	return &PrintSink{ID: id}
}

type PrintSinkEvent []byte

func (s *PrintSink) Collect(ctx context.Context, event PrintSinkEvent) {
	subject, ok := ctx.Value(rxn.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}
	subject.AddSinkRequest(s.ID, event)
}

func (s *PrintSink) isSink() {}

func (s *PrintSink) construct() (string, construct) {
	return s.ID, construct{
		Type:   "Sink:Print",
		Params: map[string]any{},
	}
}

var _ Sink = (*PrintSink)(nil)
