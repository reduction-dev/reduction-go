package memory_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/memory"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSinkSynthesize(t *testing.T) {
	job := &topology.Job{}
	sink := memory.NewSink[[]byte](job, "test-sink")

	synth := sink.Synthesize()
	assert.Equal(t, &jobconfigpb.Sink{
		Id: "test-sink",
		Config: &jobconfigpb.Sink_Memory{
			Memory: &jobconfigpb.MemorySink{},
		},
	}, synth.Config)
}
