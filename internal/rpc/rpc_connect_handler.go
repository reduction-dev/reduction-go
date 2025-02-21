package rpc

import (
	"context"

	"connectrpc.com/connect"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/handlerpb/handlerpbconnect"
)

// Receive connect requests and invoke the user's handler methods.
type ConnectHandler struct {
	rxnHandler *internal.SynthesizedHandler
}

func NewConnectHandler(handler *internal.SynthesizedHandler) *ConnectHandler {
	return &ConnectHandler{handler}
}

func (r *ConnectHandler) KeyEventBatch(ctx context.Context, req *connect.Request[handlerpb.KeyEventBatchRequest]) (*connect.Response[handlerpb.KeyEventBatchResponse], error) {
	resp, err := r.rxnHandler.KeyEventBatch(ctx, req.Msg)
	if err != nil {
		return nil, handleError(err)
	}
	return connect.NewResponse(resp), nil
}

func (r *ConnectHandler) ProcessEventBatch(ctx context.Context, req *connect.Request[handlerpb.ProcessEventBatchRequest]) (*connect.Response[handlerpb.ProcessEventBatchResponse], error) {
	resp, err := r.rxnHandler.ProcessEventBatch(ctx, req.Msg)
	if err != nil {
		return nil, handleError(err)
	}

	return connect.NewResponse(resp), nil
}

func handleError(err error) error {
	if rxnErr, ok := err.(*internal.Error); ok {
		return connect.NewError(connect.CodeInvalidArgument, rxnErr)
	}
	return err
}

var _ handlerpbconnect.HandlerHandler = (*ConnectHandler)(nil)
