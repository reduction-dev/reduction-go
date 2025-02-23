package internal

import (
	"context"

	"reduction.dev/reduction-protocol/jobconfigpb"
)

type Source interface {
	Synthesize() SourceSynthesis
	Connect(operator *Operator)
}

type SourceSynthesis struct {
	// TODO: Remove Construct after pb migration
	Construct    Construct
	KeyEventFunc func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	Operators    []*Operator
	Config       *jobconfigpb.Source
}
