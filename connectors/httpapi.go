package connectors

import (
	"context"
	"encoding/json"
	"log"

	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxn"
)

// Sink Buildtime Config

type HTTPAPISink struct {
	id   string
	addr string
}

func NewHTTPAPISink(ctx *jobs.JobContext, id string, params *HTTPAPISinkParams) *HTTPAPISink {
	sink := &HTTPAPISink{
		id:   id,
		addr: params.Addr,
	}
	ctx.RegisterSink(sink)
	return sink
}

type HTTPAPISinkParams struct {
	Addr string
}

func (s *HTTPAPISink) Synthesize() types.SinkSynthesis {
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

func (s *HTTPAPISink) Runtime(ctx *types.OperatorContext) *HTTPAPISinkRuntime {
	sink := &HTTPAPISinkRuntime{ID: s.id}
	ctx.RegisterSink(s)
	return sink
}

var _ types.SinkRuntime[*HTTPSinkEvent] = (*HTTPAPISinkRuntime)(nil)

// Sink Runtime

type HTTPAPISinkRuntime struct {
	ID string
}

type HTTPSinkEvent struct {
	// A namespace for writing the record
	Topic string
	// Arbitrary data to send to the server
	Data []byte
}

func (s *HTTPAPISinkRuntime) Collect(ctx context.Context, event *HTTPSinkEvent) {
	subject, ok := ctx.Value(internal.SubjectContextKey).(*rxn.Subject)
	if !ok {
		panic("must pass rxn context to sink.Collect")
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Fatal("httpapi Sink json.Marshal", "err", err)
	}
	subject.AddSinkRequest(s.ID, payload)
}

// Source Buildtime Config

type HTTPAPISource struct {
	id        string
	Addr      string
	Topics    []string
	KeyEvent  func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
	operators []*types.Operator
}

type HTTPAPISourceConfig struct {
	Addr     string
	Topics   []string
	KeyEvent func(ctx context.Context, record []byte) ([]types.KeyedEvent, error)
}

type HTTPAPISourceBuilder func() *HTTPAPISourceConfig

func NewHTTPAPISource(ctx *jobs.JobContext, id string, builder HTTPAPISourceBuilder) *HTTPAPISource {
	config := builder()
	source := &HTTPAPISource{
		id:       id,
		Addr:     config.Addr,
		Topics:   config.Topics,
		KeyEvent: config.KeyEvent,
	}
	ctx.RegisterSource(source)
	return source
}

func (s *HTTPAPISource) AddOperator(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *HTTPAPISource) Synthesize() types.SourceSynthesis {
	return types.SourceSynthesis{
		Construct: types.Construct{
			ID:   s.id,
			Type: "Source:HTTPAPI",
			Params: map[string]any{
				"Addr":   s.Addr,
				"Topics": s.Topics,
			},
		},
		KeyEventFunc: s.KeyEvent,
		Operators:    s.operators,
	}
}
