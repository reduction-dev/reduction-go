package httpapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/httpapi"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSourceSynthesize(t *testing.T) {
	job := &topology.Job{}
	source := httpapi.NewSource(job, "test-source", &httpapi.SourceParams{
		Addr:   topology.StringValue(":8080"),
		Topics: []string{"events", "logs"},
		KeyEvent: func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error) {
			return nil, nil
		},
	})

	synth := source.Synthesize()
	assert.Equal(t, &jobconfigpb.Source{
		Id: "test-source",
		Config: &jobconfigpb.Source_HttpApi{
			HttpApi: &jobconfigpb.HTTPAPISource{
				Addr:   topology.StringValue(":8080").Proto(),
				Topics: []string{"events", "logs"},
			},
		},
	}, synth.Config)
}
