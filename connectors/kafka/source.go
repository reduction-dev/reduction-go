package kafka

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction-protocol/kafkapb"
)

type Source struct {
	id            string
	consumerGroup topology.ResolvableString
	brokers       topology.ResolvableString
	topics        topology.ResolvableString
	keyEvent      func(ctx context.Context, record *Record) ([]internal.KeyedEvent, error)
	operators     []*internal.Operator
}

type Record struct {
	Key       []byte
	Value     []byte
	Topic     string
	Headers   []Header
	Timestamp time.Time
}

type Header struct {
	Key   string
	Value []byte
}

type SourceParams struct {
	ConsumerGroup topology.ResolvableString
	Brokers       topology.ResolvableString
	Topics        topology.ResolvableString
	KeyEvent      func(ctx context.Context, record *Record) ([]internal.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:            id,
		consumerGroup: params.ConsumerGroup,
		brokers:       params.Brokers,
		topics:        params.Topics,
		keyEvent:      params.KeyEvent,
	}
	topology.InternalAccess(job).RegisterSource(source)
	return source
}

func (s *Source) Connect(operator *internal.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *Source) Synthesize() internal.SourceSynthesis {
	return internal.SourceSynthesis{
		KeyEventFunc: func(ctx context.Context, record []byte) ([]internal.KeyedEvent, error) {
			var pbRecord kafkapb.Record
			if err := proto.Unmarshal(record, &pbRecord); err != nil {
				return nil, internal.NewBadRequestErrorf("failed to unmarshal record: %v", err)
			}

			headers := make([]Header, len(pbRecord.GetHeaders()))
			for i, header := range pbRecord.GetHeaders() {
				headers[i] = Header{
					Key:   header.GetKey(),
					Value: header.GetValue(),
				}
			}

			return s.keyEvent(ctx, &Record{
				Key:       pbRecord.GetKey(),
				Value:     pbRecord.GetValue(),
				Topic:     pbRecord.GetTopic(),
				Headers:   headers,
				Timestamp: pbRecord.GetTimestamp().AsTime(),
			})
		},
		Operators: s.operators,
		Config: &jobconfigpb.Source{
			Id: s.id,
			Config: &jobconfigpb.Source_Kafka{
				Kafka: &jobconfigpb.KafkaSource{
					ConsumerGroup: s.consumerGroup.Proto(),
					Brokers:       s.brokers.Proto(),
					Topics:        s.topics.Proto(),
				},
			},
		},
	}
}

var _ internal.Source = (*Source)(nil)
