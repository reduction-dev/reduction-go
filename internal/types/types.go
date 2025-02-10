package types

import (
	"context"
	"time"

	"reduction.dev/reduction-go/internal"
)

type Source interface {
	Synthesize() SourceSynthesis
	Connect(operator *Operator)
}

type Sink interface {
	Synthesize() SinkSynthesis
	Runtime(ctx *OperatorContext) SinkRuntime[any]
}

type SinkRuntime[T any] interface {
	Collect(ctx context.Context, value T)
}

type KeyedEvent struct {
	Key       []byte
	Timestamp time.Time
	Value     []byte
}

type SourceSynthesis struct {
	Construct    Construct
	KeyEventFunc func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	Operators    []*Operator
}

type SinkSynthesis struct {
	Construct Construct
}

// Construct defines the format for each abstract type
type Construct struct {
	ID     string
	Type   string
	Params map[string]any
}

type OperatorHandler interface {
	// Called when a new event arrives. The subject is a set of APIs scoped to
	// the specific partition key being used. Because of this scoping, think of this
	// as the subject (e.g. a User, a Product) in your domain.
	OnEvent(ctx context.Context, subject *internal.Subject, rawEvent []byte) error

	// A previously set timer expires. This is an asynchronous action where the
	// timer fires at the specified time AT THE EARLIEST. That means that events
	// after the timer's timestamp have likely already arrived.
	OnTimerExpired(ctx context.Context, subject *internal.Subject, timer time.Time) error
}

type OperatorSynthesis struct {
	Handler OperatorHandler
}

type JobSynthesis struct {
	Handler *SynthesizedHandler
	Config  interface{ Marshal() []byte }
}

type SynthesizedHandler struct {
	KeyEventFunc    func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	OperatorHandler OperatorHandler
}

func (s *SynthesizedHandler) KeyEvent(ctx context.Context, record []byte) ([]KeyedEvent, error) {
	return s.KeyEventFunc(ctx, record)
}

func (s *SynthesizedHandler) OnEvent(ctx context.Context, subject *internal.Subject, rawEvent []byte) error {
	return s.OperatorHandler.OnEvent(ctx, subject, rawEvent)
}

func (s *SynthesizedHandler) OnTimerExpired(ctx context.Context, subject *internal.Subject, timer time.Time) error {
	return s.OperatorHandler.OnTimerExpired(ctx, subject, timer)
}

type OperatorContext struct {
	// Add context fields here if needed
	Sinks []SinkSynthesizer
}

type SinkSynthesizer interface {
	Synthesize() SinkSynthesis
}

func (o *OperatorContext) RegisterSink(sink SinkSynthesizer) {
	o.Sinks = append(o.Sinks, sink)
}
