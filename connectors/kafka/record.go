package kafka

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction-protocol/kafkapb"
)

// Record is a Kafka record that's used with the Kafka source and sink.
type Record struct {
	Topic     string
	Partition int
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

type Header struct {
	Key   string
	Value []byte
}

// proto converts the Record to a protobuf representation.
func (r *Record) proto() *kafkapb.Record {
	headers := make([]*kafkapb.Header, len(r.Headers))
	for i, header := range r.Headers {
		headers[i] = &kafkapb.Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}

	return &kafkapb.Record{
		Key:       r.Key,
		Value:     r.Value,
		Topic:     r.Topic,
		Partition: int32(r.Partition),
		Timestamp: timestamppb.New(r.Timestamp),
		Headers:   headers,
	}
}

func newRecordFromProto(pbRecord *kafkapb.Record) *Record {
	headers := make([]Header, len(pbRecord.GetHeaders()))
	for i, header := range pbRecord.GetHeaders() {
		headers[i] = Header{
			Key:   header.GetKey(),
			Value: header.GetValue(),
		}
	}

	return &Record{
		Key:       pbRecord.GetKey(),
		Value:     pbRecord.GetValue(),
		Topic:     pbRecord.GetTopic(),
		Partition: int(pbRecord.GetPartition()),
		Timestamp: pbRecord.GetTimestamp().AsTime(),
		Headers:   headers,
	}
}
