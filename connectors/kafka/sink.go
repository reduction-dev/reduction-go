package kafka

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
)

type Sink struct {
	id      string
	brokers topology.ResolvableString
}

type SinkParams struct {
	Brokers topology.ResolvableString
}

func NewSink(job *topology.Job, id string, params *SinkParams) *Sink {
	sink := &Sink{
		id:      id,
		brokers: params.Brokers,
	}
	topology.InternalAccess(job).RegisterSink(sink)
	return sink
}

func (s *Sink) Synthesize() internal.SinkSynthesis {
	return internal.SinkSynthesis{
		Config: &jobconfigpb.Sink{
			Id: s.id,
			Config: &jobconfigpb.Sink_Kafka{
				Kafka: &jobconfigpb.KafkaSink{
					Brokers: s.brokers.Proto(),
				},
			},
		},
	}
}

func (s *Sink) Collect(ctx context.Context, record *Record) {
	payload, err := proto.Marshal(record.proto())
	if err != nil {
		panic(fmt.Errorf("failed to marshal record: %v", err))
	}

	subject := internal.SubjectFromContext(ctx)
	subject.AddSinkRequest(s.id, payload)
}
