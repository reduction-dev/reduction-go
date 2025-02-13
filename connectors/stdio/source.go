package stdio

import (
	"context"

	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
)

// Source reads events from stdin
type Source struct {
	id        string
	keyEvent  func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators []*types.Operator
	framing   Framing
}

type Framing struct {
	Delimiter []byte
}

type SourceParams struct {
	KeyEvent func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	Framing  Framing
}

func NewSource(job *jobs.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:       id,
		keyEvent: params.KeyEvent,
		framing:  params.Framing,
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
			Type: "Source:Stdio",
			Params: map[string]any{
				"ID": s.id,
				"Framing": map[string]any{
					"Delimiter": s.framing.Delimiter,
				},
			},
		},
		KeyEventFunc: s.keyEvent,
		Operators:    s.operators,
	}
}
