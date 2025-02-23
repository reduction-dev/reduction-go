package topology_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"reduction.dev/reduction-go/connectors/stdio"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/rxn"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestJobSynthesizeConfig(t *testing.T) {
	job := &topology.Job{
		WorkerCount:              1,
		KeyGroupCount:            2,
		WorkingStorageLocation:   "/tmp/work",
		SavepointStorageLocation: "/tmp/save",
	}

	sink := stdio.NewSink(job, "test-sink")
	source := stdio.NewSource(job, "test-source", &stdio.SourceParams{
		KeyEvent: func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error) {
			return []internal.KeyedEvent{{
				Key:       []byte("test"),
				Value:     record,
				Timestamp: time.Now(),
			}}, nil
		},
		Framing: stdio.Framing{
			Delimiter: []byte("\n"),
		},
	})
	operator := topology.NewOperator(job, "test-operator", &topology.OperatorParams{
		Parallelism: 1,
		Handler: func(op *topology.Operator) rxn.OperatorHandler {
			return nil
		},
	})
	source.Connect(operator)
	operator.Connect(sink)

	// Synthesize the job
	synth, err := job.Synthesize()
	require.NoError(t, err, "job.Synthesize() failed")

	// Marshal to JSON bytes
	jsonBytes := synth.Config.Marshal()
	t.Log(string(jsonBytes))

	// Parse back into protobuf
	var jobConfig jobconfigpb.JobConfig
	err = protojson.Unmarshal(jsonBytes, &jobConfig)
	require.NoError(t, err, "failed to parse JSON into JobConfig")

	want := &jobconfigpb.JobConfig{
		Job: &jobconfigpb.Job{
			WorkerCount:              1,
			KeyGroupCount:            2,
			WorkingStorageLocation:   "/tmp/work",
			SavepointStorageLocation: "/tmp/save",
		},
		Sources: []*jobconfigpb.Source{{
			Id: "test-source",
			Config: &jobconfigpb.Source_Stdio{
				Stdio: &jobconfigpb.StdioSource{
					Framing: &jobconfigpb.Framing{
						Delimiter: []byte("\n"),
					},
				},
			},
		}},
		Sinks: []*jobconfigpb.Sink{{
			Id: "test-sink",
			Config: &jobconfigpb.Sink_Stdio{
				Stdio: &jobconfigpb.StdioSink{},
			},
		}},
	}

	assert.EqualExportedValues(t, want, &jobConfig)
}
