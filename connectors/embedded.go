package connectors

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type EmbeddedSource struct {
	id         string
	splitCount int
	batchSize  int
	generator  string
	keyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators  []*types.Operator
}

type EmbeddedSourceParams struct {
	SplitCount int
	BatchSize  int
	Generator  string
	KeyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
}

func NewEmbeddedSource(job *jobs.Job, id string, params *EmbeddedSourceParams) *EmbeddedSource {
	source := &EmbeddedSource{
		id:         id,
		splitCount: params.SplitCount,
		batchSize:  params.BatchSize,
		generator:  params.Generator,
		keyEvent:   params.KeyEvent,
	}
	job.RegisterSource(source)
	return source
}

func (s *EmbeddedSource) Connect(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *EmbeddedSource) Synthesize() types.SourceSynthesis {
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

var _ types.Source = (*EmbeddedSource)(nil)
