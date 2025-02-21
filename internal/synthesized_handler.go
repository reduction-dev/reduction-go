package internal

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/handlerpb"
)

type SynthesizedHandler struct {
	KeyEventFunc    func(ctx context.Context, record []byte) ([]KeyedEvent, error)
	OperatorHandler OperatorHandler
}

func (s *SynthesizedHandler) KeyEvent(ctx context.Context, record []byte) ([]KeyedEvent, error) {
	return s.KeyEventFunc(ctx, record)
}

func (s *SynthesizedHandler) ProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) (*handlerpb.ProcessEventBatchResponse, error) {
	subjectBatch := NewLazySubjectBatch(req.KeyStates, req.Watermark.AsTime())

	for _, event := range req.Events {
		switch typedEvent := event.Event.(type) {
		case *handlerpb.Event_KeyedEvent:
			subject := subjectBatch.SubjectFor(typedEvent.KeyedEvent.Key, typedEvent.KeyedEvent.Timestamp.AsTime())
			ctx = ContextWithSubject(ctx, subject)
			if err := s.OperatorHandler.OnEvent(ctx, subject, KeyedEvent{
				Key:       typedEvent.KeyedEvent.Key,
				Timestamp: typedEvent.KeyedEvent.Timestamp.AsTime(),
				Value:     typedEvent.KeyedEvent.Value,
			}); err != nil {
				return nil, err
			}
		case *handlerpb.Event_TimerExpired:
			subject := subjectBatch.SubjectFor(typedEvent.TimerExpired.Key, typedEvent.TimerExpired.Timestamp.AsTime())
			ctx = ContextWithSubject(ctx, subject)
			if err := s.OperatorHandler.OnTimerExpired(ctx, subject, typedEvent.TimerExpired.Timestamp.AsTime()); err != nil {
				return nil, err
			}
		}
	}

	return subjectBatch.Response(), nil
}

func (s *SynthesizedHandler) KeyEventBatch(ctx context.Context, req *handlerpb.KeyEventBatchRequest) (*handlerpb.KeyEventBatchResponse, error) {
	results := make([]*handlerpb.KeyEventResult, len(req.Values))
	for valueIdx, value := range req.Values {
		keyedEvents, err := s.KeyEvent(ctx, value)
		if err != nil {
			return nil, err
		}

		pbKeyedEvents := make([]*handlerpb.KeyedEvent, len(keyedEvents))
		for eventIdx, event := range keyedEvents {
			pbKeyedEvents[eventIdx] = &handlerpb.KeyedEvent{
				Key:       event.Key,
				Value:     event.Value,
				Timestamp: timestamppb.New(event.Timestamp),
			}
		}

		results[valueIdx] = &handlerpb.KeyEventResult{Events: pbKeyedEvents}
	}

	return &handlerpb.KeyEventBatchResponse{Results: results}, nil
}
