package kinesis

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/jobconfigpb"
	"reduction.dev/reduction-protocol/kinesispb"
)

type Source struct {
	id        string
	streamARN topology.ResolvableString
	endpoint  topology.ResolvableString
	keyEvent  func(ctx context.Context, record *Record) ([]internal.KeyedEvent, error)
	operators []*internal.Operator
}

type Record struct {
	Timestamp time.Time
	Data      []byte
}

type SourceParams struct {
	StreamARN topology.ResolvableString
	Endpoint  topology.ResolvableString
	KeyEvent  func(ctx context.Context, record *Record) ([]internal.KeyedEvent, error)
}

func NewSource(job *topology.Job, id string, params *SourceParams) *Source {
	source := &Source{
		id:        id,
		streamARN: params.StreamARN,
		endpoint:  params.Endpoint,
		keyEvent:  params.KeyEvent,
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
			var pbRecord kinesispb.Record
			if err := proto.Unmarshal(record, &pbRecord); err != nil {
				return nil, internal.NewBadRequestErrorf("failed to unmarshal record: %v", err)
			}
			kinesisRecord := &Record{
				Data:      pbRecord.Data,
				Timestamp: pbRecord.Timestamp.AsTime(),
			}
			return s.keyEvent(ctx, kinesisRecord)
		},
		Operators: s.operators,
		Config: &jobconfigpb.Source{
			Id: s.id,
			Config: &jobconfigpb.Source_Kinesis{
				Kinesis: &jobconfigpb.KinesisSource{
					StreamArn: s.streamARN.Proto(),
					Endpoint:  s.endpoint.Proto(),
				},
			},
		},
	}
}

var _ internal.Source = (*Source)(nil)
