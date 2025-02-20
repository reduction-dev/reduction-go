package embedded

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/topology"
)

type Source struct {
	id         string
	splitCount int
	batchSize  int
	generator  string
	keyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators  []*types.Operator
}

type SourceParams struct {
	SplitCount int
	BatchSize  int
	Generator  string
	KeyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:         id,
		splitCount: params.SplitCount,
		batchSize:  params.BatchSize,
		generator:  params.Generator,
		keyEvent:   params.KeyEvent,
	}
	job.RegisterSource(source)
	return source
}

func (s *Source) Connect(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *Source) Synthesize() types.SourceSynthesis {
	return types.SourceSynthesis{
		Construct: types.Construct{
			ID:   s.id,
			Type: "Source:Embedded",
			Params: map[string]any{
				"SplitCount": s.splitCount,
				"BatchSize":  s.batchSize,
				"Generator":  s.generator,
			},
		},
		KeyEventFunc: s.keyEvent,
		Operators:    s.operators,
	}
}

var _ types.Source = (*Source)(nil)
