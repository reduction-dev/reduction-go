package httpapi

import (
	"context"
	"encoding/json"
	"fmt"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
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
	topology.InternalAccess(job).RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() internal.SinkSynthesis {
	return internal.SinkSynthesis{
		Construct: internal.Construct{
			ID:   s.id,
			Type: "Sink:HTTPAPI",
			Params: map[string]any{
				"Addr": s.addr,
			},
		},
		Config: &jobconfigpb.Sink{
			Id: s.id,
			Config: &jobconfigpb.Sink_HttpApi{
				HttpApi: &jobconfigpb.HTTPAPISink{
					Addr: s.addr,
				},
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, value *SinkRecord) {
	subject := internal.SubjectFromContext(ctx)

	payload, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("httpapi Sink json.Marshal: %v", err))
	}
	subject.AddSinkRequest(s.id, payload)
}

// Source Buildtime Config

type Source struct {
	id        string
	addr      string
	topics    []string
	keyEvent  func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error)
	operators []*internal.Operator
}

type SourceParams struct {
	Addr     string
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
		Construct: internal.Construct{
			ID:   s.id,
			Type: "Source:HTTPAPI",
			Params: map[string]any{
				"Addr":   s.addr,
				"Topics": s.topics,
			},
		},
		KeyEventFunc: s.keyEvent,
		Operators:    s.operators,
		Config: &jobconfigpb.Source{
			Id: s.id,
			Config: &jobconfigpb.Source_HttpApi{
				HttpApi: &jobconfigpb.HTTPAPISource{
					Addr:   s.addr,
					Topics: s.topics,
				},
			},
		},
	}
}
