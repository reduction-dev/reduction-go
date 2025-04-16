package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction-go/connectors/kafka"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

func TestSinkSynthesize(t *testing.T) {
	job := &topology.Job{}
	sink := kafka.NewSink(job, "test-sink", &kafka.SinkParams{
		Brokers: topology.StringValue("broker1,broker2"),
	})

	synth := sink.Synthesize()
	assert.Equal(t, &jobconfigpb.Sink{
		Id: "test-sink",
		Config: &jobconfigpb.Sink_Kafka{
			Kafka: &jobconfigpb.KafkaSink{
				Brokers: topology.StringValue("broker1,broker2").Proto(),
			},
		},
	}, synth.Config)
}
