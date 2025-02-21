package stdio

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
)

// Source reads events from stdin
type Source struct {
	id        string
	keyEvent  func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
	operators []*internal.Operator
	framing   Framing
}

type Framing struct {
	Delimiter []byte
}

type SourceParams struct {
	KeyEvent func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
	Framing  Framing
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:       id,
		keyEvent: params.KeyEvent,
		framing:  params.Framing,
	}
	topology.InternalAccess(job).RegisterSource(source)
	return source
}

func (s *Source) Connect(operator *internal.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *Source) Synthesize() internal.SourceSynthesis {
	return internal.SourceSynthesis{
		Construct: internal.Construct{
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
