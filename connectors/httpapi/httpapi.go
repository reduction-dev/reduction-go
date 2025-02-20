package httpapi

import (
	"context"
	"encoding/json"
	"log"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
)

// Sink Buildtime Config

type Sink struct {
	id   string
	addr string
}

type SinkParams struct {
	Addr string
}

type SinkRecord struct {
	// A namespace for writing the record
	Topic string
	// Arbitrary data to send to the server
	Data []byte
}

func NewSink(job *topology.Job, id string, params *SinkParams) *Sink {
	sink := &Sink{
		id:   id,
		addr: params.Addr,
	}
	job.RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() types.SinkSynthesis {
	return types.SinkSynthesis{
		Construct: types.Construct{
			ID:   s.id,
			Type: "Sink:HTTPAPI",
			Params: map[string]any{
				"Addr": s.addr,
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, value *SinkRecord) {
	subject, ok := ctx.Value(internal.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}

	payload, err := json.Marshal(value)
	if err != nil {
		log.Fatal("httpapi Sink json.Marshal", "err", err)
	}
	subject.AddSinkRequest(s.id, payload)
}

var _ types.SinkRuntime[*SinkRecord] = (*Sink)(nil)

// Source Buildtime Config

type Source struct {
	id        string
	addr      string
	topics    []string
	keyEvent  func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators []*types.Operator
}

type SourceParams struct {
	Addr     string
	Topics   []string
	KeyEvent func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:       id,
		addr:     params.Addr,
		topics:   params.Topics,
		keyEvent: params.KeyEvent,
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
			Type: "Source:HTTPAPI",
			Params: map[string]any{
				"Addr":   s.addr,
				"Topics": s.topics,
			},
		},
		KeyEventFunc: s.keyEvent,
		Operators:    s.operators,
	}
}
