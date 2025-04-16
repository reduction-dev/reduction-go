package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-go/connectors/kafka"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction-protocol/kafkapb"
)

func TestSourceSynthesize(t *testing.T) {
	job := &topology.Job{}
	source := kafka.NewSource(job, "test-source", &kafka.SourceParams{
		ConsumerGroup: topology.StringValue("test-group"),
		Brokers:       topology.StringValue("localhost:9092"),
		Topics:        topology.StringValueList("topic1", "topic2"),
		KeyEvent:      func(ctx context.Context, record *kafka.Record) ([]internal.KeyedEvent, error) { return nil, nil },
	})

	synth := source.Synthesize()
	assert.Equal(t, &jobconfigpb.Source{
		Id: "test-source",
		Config: &jobconfigpb.Source_Kafka{
			Kafka: &jobconfigpb.KafkaSource{
				ConsumerGroup: &jobconfigpb.StringVar{
					Kind: &jobconfigpb.StringVar_Value{Value: "test-group"},
				},
				Brokers: &jobconfigpb.StringVar{
					Kind: &jobconfigpb.StringVar_Value{Value: "localhost:9092"},
				},
				Topics: &jobconfigpb.StringVar{
					Kind: &jobconfigpb.StringVar_Value{Value: "topic1,topic2"},
				},
			},
		},
	}, synth.Config)
}

func TestSource_KeyEventFunc(t *testing.T) {
	timestamp := time.Now().UTC()

	pbRecord := &kafkapb.Record{
		Key:   []byte("key"),
		Value: []byte("value"),
		Topic: "topic",
		Headers: []*kafkapb.Header{{
			Key:   "header-key",
			Value: []byte("value-key"),
		}},
		Timestamp: timestamppb.New(timestamp),
	}
	data, err := proto.Marshal(pbRecord)
	require.NoError(t, err, "failed to marshal pbRecord")

	var kafkaRecord *kafka.Record
	source := kafka.NewSource(&topology.Job{}, "test-source", &kafka.SourceParams{
		ConsumerGroup: topology.StringValue("test-group"),
		Brokers:       topology.StringValue("localhost:9092"),
		Topics:        topology.StringValueList("topic"),
		KeyEvent: func(ctx context.Context, record *kafka.Record) ([]internal.KeyedEvent, error) {
			kafkaRecord = record
			return nil, nil
		},
	})

	_, err = source.Synthesize().KeyEventFunc(context.Background(), data)
	require.NoError(t, err)

	assert.Equal(t, kafkaRecord, &kafka.Record{
		Key:       pbRecord.Key,
		Value:     pbRecord.Value,
		Topic:     pbRecord.Topic,
		Headers:   []kafka.Header{{Key: "header-key", Value: []byte("value-key")}},
		Timestamp: pbRecord.Timestamp.AsTime(),
	})
}
