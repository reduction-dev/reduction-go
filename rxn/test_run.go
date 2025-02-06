package rxn

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"reduction.dev/reduction-go/jobs"
)

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

func TestRun(job *jobs.Job, events [][]byte) error {
	cmd := exec.Command("reduction", "testrun")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
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

	// Write events
	for _, event := range events {
		if err := binary.Write(stdin, binary.BigEndian, uint32(len(event))); err != nil {
			return fmt.Errorf("failed to write event length: %w", err)
		}
		if _, err := stdin.Write(event); err != nil {
			return fmt.Errorf("failed to write event data: %w", err)
		}
	}

	// Write zero byte to signal end of events
	if err := binary.Write(stdin, binary.BigEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write delimiter: %w", err)
	}

	handler, err := job.Synthesize()
	if err != nil {
		return fmt.Errorf("failed to synthesize job: %w", err)
	}

	// Process messages using pipe handler
	pipeHandler := newRPCPipeHandler(handler, stdin, stdout)
	if err := pipeHandler.ProcessMessages(context.Background()); err != nil {
		return err
	}

	// Wait for stderr collection to complete
	stderrWG.Wait()

	// Check command exit status, including stderr if there was an error
	if err := cmd.Wait(); err != nil {
		return &commandError{
			err:    err,
			stderr: stderr.Bytes(),
		}
	}

	return nil
}
