package httpapi

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

type Source struct {
	id        string
	addr      topology.ResolvableString
	topics    []string
	keyEvent  func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
	operators []*internal.Operator
}

type SourceParams struct {
	Addr     topology.ResolvableString
	Topics   []string
	KeyEvent func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:       id,
		addr:     params.Addr,
		topics:   params.Topics,
		keyEvent: params.KeyEvent,
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
			Config: &jobconfigpb.Source_HttpApi{
				HttpApi: &jobconfigpb.HTTPAPISource{
					Addr:   s.addr.Proto(),
					Topics: s.topics,
				},
			},
		},
	}
}
