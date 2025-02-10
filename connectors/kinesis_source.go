package connectors

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"reduction.dev/reduction-go/internal/types"
	"reduction.dev/reduction-go/jobs"
	"reduction.dev/reduction-go/rxnerr"
	"reduction.dev/reduction-handler/kinesispb"
)

type KinesisSource struct {
	id        string
	streamARN string
	endpoint  string
	keyEvent  func(ctx context.Context, record *KinesisRecord) ([]types.KeyedEvent, error)
	operators []*types.Operator
}

type KinesisRecord struct {
	Timestamp time.Time
	Data      []byte
}

type KinesisSourceParams struct {
	StreamARN string
	Endpoint  string
	KeyEvent  func(ctx context.Context, record *KinesisRecord) ([]types.KeyedEvent, error)
}

func NewKinesisSource(job *jobs.Job, id string, params *KinesisSourceParams) *KinesisSource {
	source := &KinesisSource{
		id:        id,
		streamARN: params.StreamARN,
		endpoint:  params.Endpoint,
		keyEvent:  params.KeyEvent,
	}
	job.RegisterSource(source)
	return source
}

func (s *KinesisSource) Connect(operator *types.Operator) {
	s.operators = append(s.operators, operator)
}

func (s *KinesisSource) Synthesize() types.SourceSynthesis {
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
				return nil, rxnerr.NewBadRequestErrorf("failed to unmarshal record: %v", err)
			}
			kinesisRecord := &KinesisRecord{
				Data:      pbRecord.Data,
				Timestamp: pbRecord.Timestamp.AsTime(),
			}
			return s.keyEvent(ctx, kinesisRecord)
		},
		Operators: s.operators,
	}
}

var _ types.Source = (*KinesisSource)(nil)
