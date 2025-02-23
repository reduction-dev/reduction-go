package embedded

import (
	"context"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

type Source struct {
	id         string
	splitCount int
	batchSize  int
	generator  generatorType
	keyEvent   func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
	operators  []*internal.Operator
}

type generatorType = jobconfigpb.EmbeddedSource_GeneratorType

const (
	GeneratorUnspecified generatorType = jobconfigpb.EmbeddedSource_GENERATOR_TYPE_UNSPECIFIED
	GeneratorSequence    generatorType = jobconfigpb.EmbeddedSource_GENERATOR_TYPE_SEQUENCE
)

type SourceParams struct {
	SplitCount int
	BatchSize  int
	Generator  generatorType
	KeyEvent   func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:         id,
		splitCount: params.SplitCount,
		batchSize:  params.BatchSize,
		generator:  params.Generator,
		keyEvent:   params.KeyEvent,
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
			Config: &jobconfigpb.Source_Embedded{
				Embedded: &jobconfigpb.EmbeddedSource{
					SplitCount: int32(s.splitCount),
					BatchSize:  int32(s.batchSize),
					Generator:  s.generator,
				},
			},
		},
	}
}

var _ internal.Source = (*Source)(nil)
