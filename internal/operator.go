package internal

import (
	"context"
	"time"
)

type OperatorParams struct {
	Handler OperatorHandler
}

type Operator struct {
	ID          string
	Parallelism int
	Handler     OperatorHandler
	Sinks       []SinkSynthesizer
	stateSpecs  map[string]QueryType
}

type QueryType = string

const QueryTypeGet QueryType = "get"
const QueryTypeScan QueryType = "scan"

func NewOperator(id string) *Operator {
	// ID is currently unused.
	return &Operator{
		stateSpecs: make(map[string]QueryType),
	}
}

func (o *Operator) Synthesize() OperatorSynthesis {
	return OperatorSynthesis{
		Handler: o.Handler,
	}
}

func (op *Operator) Connect(sink SinkSynthesizer) {
	op.Sinks = append(op.Sinks, sink)
}

func (op *Operator) RegisterSpec(id string, queryType QueryType) {
	op.stateSpecs[id] = queryType
}

type OperatorSynthesis struct {
	Handler OperatorHandler
}

type SynthesizedHandler struct {
	KeyEventFunc    func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	OperatorHandler OperatorHandler
}

func (s *SynthesizedHandler) KeyEvent(ctx context.Context, record []byte) ([]KeyedEvent, error) {
	return s.KeyEventFunc(ctx, record)
}

func (s *SynthesizedHandler) OnEvent(ctx context.Context, subject *Subject, event KeyedEvent) error {
	return s.OperatorHandler.OnEvent(ctx, subject, event)
}

func (s *SynthesizedHandler) OnTimerExpired(ctx context.Context, subject *Subject, timer time.Time) error {
	return s.OperatorHandler.OnTimerExpired(ctx, subject, timer)
}
