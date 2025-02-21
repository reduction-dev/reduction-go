package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/testrunpb"
)

// Receive messages over a unix pipe and invoke the user's handler methods.
type PipeHandler struct {
	rxnHandler *internal.SynthesizedHandler
	stdin      io.Writer
	stdout     io.Reader
}

func NewPipeHandler(handler *internal.SynthesizedHandler, stdin io.Writer, stdout io.Reader) *PipeHandler {
	return &PipeHandler{
		rxnHandler: handler,
		stdin:      stdin,
		stdout:     stdout,
	}
}

// Read messages each containing a method and request parameter.
func (r *PipeHandler) readMessage() ([]byte, error) {
	// Read message length
	var length uint32
	if err := binary.Read(r.stdout, binary.BigEndian, &length); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	// Read message data
	data := make([]byte, length)
	if _, err := io.ReadFull(r.stdout, data); err != nil {
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	return data, nil
}

// Write responses with only the proto response type.
func (r *PipeHandler) writeResponse(msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	if err := binary.Write(r.stdin, binary.BigEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write response length: %w", err)
	}

	if _, err := r.stdin.Write(data); err != nil {
		return fmt.Errorf("failed to write response data: %w", err)
	}

	return nil
}

// ProcessMessages handles all messages until reaching EOF.
func (r *PipeHandler) ProcessMessages(ctx context.Context) error {
	for {
		data, err := r.readMessage()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		var cmd testrunpb.HandlerCommand
		if err := proto.Unmarshal(data, &cmd); err != nil {
			return fmt.Errorf("failed to unmarshal HandlerCommand: %w", err)
		}

		switch c := cmd.Command.(type) {
		case *testrunpb.HandlerCommand_KeyEventBatch:
			if err := r.handleKeyEventBatch(ctx, c.KeyEventBatch.KeyEventBatchRequest); err != nil {
				return err
			}
		case *testrunpb.HandlerCommand_ProcessEventBatch:
			if err := r.handleProcessEventBatch(ctx, c.ProcessEventBatch.ProcessEventBatchRequest); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown command type: %T", c)
		}
	}
}

func (r *PipeHandler) handleKeyEventBatch(ctx context.Context, req *handlerpb.KeyEventBatchRequest) error {
	resp, err := r.rxnHandler.KeyEventBatch(ctx, req)
	if err != nil {
		return err
	}
	return r.writeResponse(resp)
}

func (r *PipeHandler) handleProcessEventBatch(ctx context.Context, req *handlerpb.ProcessEventBatchRequest) error {
	resp, err := r.rxnHandler.ProcessEventBatch(ctx, req)
	if err != nil {
		return err
	}
	return r.writeResponse(resp)
}
