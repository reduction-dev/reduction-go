package rxn

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction-handler/handlerpb/handlerpbconnect"
)

// Receive connect requests and invoke the user's handler methods.
type rpcHandler struct {
	rxnHandler Handler
}

func (r *rpcHandler) KeyEventBatch(ctx context.Context, req *connect.Request[handlerpb.KeyEventBatchRequest]) (*connect.Response[handlerpb.KeyEventBatchResponse], error) {
	results := make([]*handlerpb.KeyEventResult, 0, len(req.Msg.Values))
	for _, value := range req.Msg.Values {
		keyedEvents, err := r.rxnHandler.KeyEvent(ctx, value)
		if err != nil {
			return nil, err
		}
		pbKeyedEvents := make([]*handlerpb.KeyedEvent, len(keyedEvents))
		for i, event := range keyedEvents {
			pbKeyedEvents[i] = &handlerpb.KeyedEvent{
				Key:       event.Key,
				Timestamp: timestamppb.New(event.Timestamp),
				Value:     event.Value,
			}
		}
		results = append(results, &handlerpb.KeyEventResult{Events: pbKeyedEvents})
	}

	return connect.NewResponse(&handlerpb.KeyEventBatchResponse{
		Results: results,
	}), nil
}

func (r *rpcHandler) ProcessEventBatch(ctx context.Context, req *connect.Request[handlerpb.ProcessEventBatchRequest]) (*connect.Response[handlerpb.ProcessEventBatchResponse], error) {
	// Track subjects by key
	subjectBatch := newLazySubjectBatch(req.Msg.KeyStates)
	watermark := req.Msg.Watermark.AsTime()

	for _, event := range req.Msg.Events {
		switch typedEvent := event.Event.(type) {
		case *handlerpb.Event_KeyedEvent:
			subject := subjectBatch.subjectFor(typedEvent.KeyedEvent.Key, typedEvent.KeyedEvent.Timestamp.AsTime())
			ctx = context.WithValue(ctx, SubjectContextKey, subject)
			ctx = context.WithValue(ctx, WatermarkContextKey, watermark)
			if err := r.rxnHandler.OnEvent(ctx, subject, typedEvent.KeyedEvent.Value); err != nil {
				return nil, err
			}
		case *handlerpb.Event_TimerExpired:
			subject := subjectBatch.subjectFor(typedEvent.TimerExpired.Key, typedEvent.TimerExpired.Timestamp.AsTime())
			ctx = context.WithValue(ctx, SubjectContextKey, subject)
			ctx = context.WithValue(ctx, WatermarkContextKey, watermark)
			if err := r.rxnHandler.OnTimerExpired(ctx, subject, typedEvent.TimerExpired.Timestamp.AsTime()); err != nil {
				return nil, err
			}
		}
	}

	resp := subjectBatch.response()
	return connect.NewResponse(resp), nil
}

var _ handlerpbconnect.HandlerHandler = (*rpcHandler)(nil)
