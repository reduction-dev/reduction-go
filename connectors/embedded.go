package connectors

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

type EmbeddedSource struct {
	id         string
	SplitCount int
	BatchSize  int
	Generator  string
	KeyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators  []*types.Operator
}

type EmbeddedSourceConfig struct {
	SplitCount int
	BatchSize  int
	Generator  string
	KeyEvent   func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
}

type EmbeddedSourceBuilder func() *EmbeddedSourceConfig

func NewEmbeddedSource(ctx *jobs.JobContext, id string, builder EmbeddedSourceBuilder) *EmbeddedSource {
	config := builder()
	source := &EmbeddedSource{
		id:         id,
		SplitCount: config.SplitCount,
		BatchSize:  config.BatchSize,
		Generator:  config.Generator,
		KeyEvent:   config.KeyEvent,
	}
	ctx.RegisterSource(source)
	return source
}

func (s *EmbeddedSource) AddOperator(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *EmbeddedSource) Synthesize() types.SourceSynthesis {
	return types.SourceSynthesis{
		Construct: types.Construct{
			ID:   s.id,
			Type: "Source:Embedded",
			Params: map[string]any{
				"SplitCount": s.SplitCount,
				"BatchSize":  s.BatchSize,
				"Generator":  s.Generator,
			},
		},
		KeyEventFunc: s.KeyEvent,
		Operators:    s.operators,
	}
}

var _ types.Source = (*EmbeddedSource)(nil)
