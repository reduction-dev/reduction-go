package internal

import "context"

type Source interface {
	Synthesize() SourceSynthesis
	Connect(operator *Operator)
}

type SourceSynthesis struct {
	Construct    Construct
	KeyEventFunc func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	Operators    []*Operator
}
