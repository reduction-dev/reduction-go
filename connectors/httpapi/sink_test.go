package httpapi_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSinkSynthesize(t *testing.T) {
	job := &topology.Job{}
	sink := httpapi.NewSink(job, "test-sink", &httpapi.SinkParams{
		Addr: topology.StringValue("http://example.com/events"),
	})

	synth := sink.Synthesize()
	assert.Equal(t, &jobconfigpb.Sink{
		Id: "test-sink",
		Config: &jobconfigpb.Sink_HttpApi{
			HttpApi: &jobconfigpb.HTTPAPISink{
				Addr: topology.StringParam("http://example.com/events").Proto(),
			},
		},
	}, synth.Config)
}
