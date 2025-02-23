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
	KeyEventFunc func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	Operators    []*Operator
	Config       *jobconfigpb.Source
}
