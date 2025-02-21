package topology

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/rpc"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction-protocol/testrunpb"
)

func (j *Job) NewTestRun() *TestRun {
	tr := &TestRun{
		commands: make([][]byte, 0),
		job:      j,
	}
	synthesis, err := j.Synthesize()
	if err != nil {
		tr.err = fmt.Errorf("failed to synthesize job: %w", err)
		return tr
	}

	tr.handler = synthesis.Handler
	return tr
}

// A TestRun accumulates commands and runs them against `reduction testrun`.
type TestRun struct {
	commands [][]byte
	job      *Job
	err      error
	handler  *internal.SynthesizedHandler
}

func (t *TestRun) AddRecord(record []byte) {
	if t.err != nil {
		return
	}

	keyedEvents, err := t.handler.KeyEvent(context.Background(), record)
	if err != nil {
		t.err = fmt.Errorf("failed to create keyed event: %w", err)
		return
	}

	for _, ke := range keyedEvents {
		cmd := &testrunpb.RunnerCommand{
			Command: &testrunpb.RunnerCommand_AddKeyedEvent{
				AddKeyedEvent: &testrunpb.AddKeyedEvent{
					KeyedEvent: &handlerpb.KeyedEvent{
						Key:       ke.Key,
						Timestamp: timestamppb.New(ke.Timestamp),
						Value:     ke.Value,
					},
				},
			},
		}
		msgData, err := proto.Marshal(cmd)
		if err != nil {
			t.err = fmt.Errorf("failed to marshal command: %w", err)
			return
		}
		t.commands = append(t.commands, msgData)
	}
}

func (t *TestRun) AddWatermark() {
	if t.err != nil {
		return
	}

	cmd := &testrunpb.RunnerCommand{
		Command: &testrunpb.RunnerCommand_AddWatermark{
			AddWatermark: &testrunpb.AddWatermark{},
		},
	}
	msgData, err := proto.Marshal(cmd)
	if err != nil {
		t.err = fmt.Errorf("failed to marshal command: %w", err)
		return
	}
	t.commands = append(t.commands, msgData)
}

func (t *TestRun) Run() error {
	if t.err != nil {
		return t.err
	}

	// Add Run command
	cmd := &testrunpb.RunnerCommand{
		Command: &testrunpb.RunnerCommand_Run{
			Run: &testrunpb.Run{},
		},
	}
	msgData, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal run command: %w", err)
	}
	t.commands = append(t.commands, msgData)

	trCmd := exec.Command("reduction", "testrun")

	stdin, err := trCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := trCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := trCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := trCmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	// Collect stderr output and pipe to os.Stderr
	var stderr bytes.Buffer
	var stderrWG sync.WaitGroup
	stderrWG.Add(1)
	go func() {
		defer stderrWG.Done()
		io.Copy(io.MultiWriter(&stderr, os.Stderr), stderrPipe)
	}()

	// Write commands
	for _, msg := range t.commands {
		if err := binary.Write(stdin, binary.BigEndian, uint32(len(msg))); err != nil {
			return fmt.Errorf("failed to write message length: %w", err)
		}
		if _, err := stdin.Write(msg); err != nil {
			return fmt.Errorf("failed to write message data: %w", err)
		}
	}

	// Process messages using pipe handler
	synthesis, err := t.job.Synthesize()
	if err != nil {
		return fmt.Errorf("failed to synthesize job: %w", err)
	}

	pipeHandler := rpc.NewPipeHandler(synthesis.Handler, stdin, stdout)
	if err := pipeHandler.ProcessMessages(context.Background()); err != nil {
		return err
	}

	// Wait for stderr collection to complete
	stderrWG.Wait()

	// Check command exit status, including stderr if there was an error
	if err := trCmd.Wait(); err != nil {
		return &commandError{
			err:    err,
			stderr: stderr.Bytes(),
		}
	}

	return nil
}

type commandError struct {
	err    error
	stderr []byte
}

func (e *commandError) Error() string {
	var b bytes.Buffer
	b.WriteString(e.err.Error())
	if len(e.stderr) > 0 {
		b.WriteString("\n\nStderr:\n")
		b.Write(e.stderr)
	}
	return b.String()
}
