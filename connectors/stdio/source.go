package stdio

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
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
		KeyEventFunc: s.keyEvent,
		Operators:    s.operators,
		Config: &jobconfigpb.Source{
			Id: s.id,
			Config: &jobconfigpb.Source_Stdio{
				Stdio: &jobconfigpb.StdioSource{
					Framing: &jobconfigpb.Framing{
						Delimiter: s.framing.Delimiter,
					},
				},
			},
		},
	}
}
