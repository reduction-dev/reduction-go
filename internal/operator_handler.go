package internal

import (
	"context"
	"time"
)

type OperatorHandler interface {
	// Called when a new event arrives. The subject is a set of APIs scoped to
	// the specific partition key being used. Because of this scoping, think of this
	// as the subject (e.g. a User, a Product) in your domain.
	OnEvent(ctx context.Context, subject *Subject, event KeyedEvent) error

	// A previously set timer expires. This is an asynchronous action where the
	// timer fires at the specified time AT THE EARLIEST. That means that events
	// after the timer's timestamp have likely already arrived.
	OnTimerExpired(ctx context.Context, subject *Subject, timer time.Time) error
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
