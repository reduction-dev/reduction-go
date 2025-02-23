package kinesis_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/kinesis"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSourceSynthesize(t *testing.T) {
	job := &topology.Job{}
	source := kinesis.NewSource(job, "test-source", &kinesis.SourceParams{
		StreamARN: "arn:aws:kinesis:us-west-2:123456789012:stream/test",
		Endpoint:  "kinesis.us-west-2.amazonaws.com",
		KeyEvent:  func(ctx context.Context, record *kinesis.Record) ([]internal.KeyedEvent, error) { return nil, nil },
	})

	synth := source.Synthesize()
	assert.Equal(t, &jobconfigpb.Source{
		Id: "test-source",
		Config: &jobconfigpb.Source_Kinesis{
			Kinesis: &jobconfigpb.KinesisSource{
				StreamArn: "arn:aws:kinesis:us-west-2:123456789012:stream/test",
				Endpoint:  "kinesis.us-west-2.amazonaws.com",
			},
		},
	}, synth.Config)
}
