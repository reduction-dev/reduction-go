package rxn

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/handlerpb/handlerpbconnect"
)

// Receive connect requests and invoke the user's handler methods.
type rpcConnectHandler struct {
	rxnHandler types.ServerHandler
}

func (r *rpcConnectHandler) KeyEventBatch(ctx context.Context, req *connect.Request[handlerpb.KeyEventBatchRequest]) (*connect.Response[handlerpb.KeyEventBatchResponse], error) {
	results := make([]*handlerpb.KeyEventResult, 0, len(req.Msg.Values))
	for _, value := range req.Msg.Values {
		keyedEvents, err := r.rxnHandler.KeyEvent(ctx, value)
		if err != nil {
			return nil, handleError(err)
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

func (r *rpcConnectHandler) ProcessEventBatch(ctx context.Context, req *connect.Request[handlerpb.ProcessEventBatchRequest]) (*connect.Response[handlerpb.ProcessEventBatchResponse], error) {
	// Track subjects by key
	subjectBatch := internal.NewLazySubjectBatch(req.Msg.KeyStates)
	watermark := req.Msg.Watermark.AsTime()

	for _, event := range req.Msg.Events {
		switch typedEvent := event.Event.(type) {
		case *handlerpb.Event_KeyedEvent:
			subject := subjectBatch.SubjectFor(typedEvent.KeyedEvent.Key, typedEvent.KeyedEvent.Timestamp.AsTime())
			ctx = context.WithValue(ctx, internal.SubjectContextKey, subject)
			ctx = context.WithValue(ctx, internal.WatermarkContextKey, watermark)
			if err := r.rxnHandler.OnEvent(ctx, subject, typedEvent.KeyedEvent.Value); err != nil {
				return nil, handleError(err)
			}
		case *handlerpb.Event_TimerExpired:
			subject := subjectBatch.SubjectFor(typedEvent.TimerExpired.Key, typedEvent.TimerExpired.Timestamp.AsTime())
			ctx = context.WithValue(ctx, internal.SubjectContextKey, subject)
			ctx = context.WithValue(ctx, internal.WatermarkContextKey, watermark)
			if err := r.rxnHandler.OnTimerExpired(ctx, subject, typedEvent.TimerExpired.Timestamp.AsTime()); err != nil {
				return nil, handleError(err)
			}
		}
	}

	resp := subjectBatch.Response()
	return connect.NewResponse(resp), nil
}

func handleError(err error) error {
	if rxnErr, ok := err.(*internal.Error); ok {
		return connect.NewError(connect.CodeInvalidArgument, rxnErr)
	}
	return err
}

var _ handlerpbconnect.HandlerHandler = (*rpcConnectHandler)(nil)
