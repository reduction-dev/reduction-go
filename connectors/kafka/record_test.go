package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRecordRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)

	record := &Record{
		Topic:     "test-topic",
		Partition: 1,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers: []Header{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
		Timestamp: now,
	}

	result := newRecordFromProto(record.proto())
	assert.Equal(t, record, result)
}
