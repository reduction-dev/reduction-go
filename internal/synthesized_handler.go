package internal

import (
	"context"
	"time"
)

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
