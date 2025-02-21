package kinesis

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/topology"
	"reduction.dev/reduction-protocol/kinesispb"
)

type Source struct {
	id        string
	streamARN string
	endpoint  string
	keyEvent  func(ctx context.Context, record *Record) ([]types.KeyedEvent, error)
	operators []*types.Operator
}

type Record struct {
	Timestamp time.Time
	Data      []byte
}

type SourceParams struct {
	StreamARN string
	Endpoint  string
	KeyEvent  func(ctx context.Context, record *Record) ([]types.KeyedEvent, error)
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

func (s *Source) Connect(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *Source) Synthesize() types.SourceSynthesis {
	return types.SourceSynthesis{
		Construct: types.Construct{
			ID:   s.id,
			Type: "Source:Kinesis",
			Params: map[string]any{
				"StreamARN": s.streamARN,
				"Endpoint":  s.endpoint,
			},
		},
		KeyEventFunc: func(ctx context.Context, record []byte) ([]types.KeyedEvent, error) {
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
	}
}

var _ types.Source = (*Source)(nil)
