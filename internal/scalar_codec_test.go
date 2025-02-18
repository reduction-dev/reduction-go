package internal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction-go/internal"
)

func TestScalarCodec(t *testing.T) {
	roundTrip(t, int(42), "int type")
	roundTrip(t, int64(9223372036854775807), "int64 type")
	roundTrip(t, uint(42), "uint type")
	roundTrip(t, uint64(18446744073709551615), "uint64 type")
	roundTrip(t, float32(3.14159), "float32 type")
	roundTrip(t, float64(3.14159), "float64 type")
	roundTrip(t, "hello world", "string type")
	roundTrip(t, true, "bool type")
	roundTrip(t, time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), "time.Time type")
}

func roundTrip[T any](t *testing.T, val T, msg string) {
	t.Helper()
	bytes, err := internal.EncodeScalar(val)
	require.NoError(t, err, "no error encoding %s", msg)
	decoded, err := internal.DecodeScalar[T](bytes)
	require.NoError(t, err, "no error decoding %s", msg)
	assert.Equal(t, val, decoded, "decoding values for %s", msg)
}

func TestScalarCodecErrors(t *testing.T) {
	// Test encoding unsupported type
	type unsupportedType struct{}
	_, err := internal.EncodeScalar(unsupportedType{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")

	// Test decoding invalid bytes
	invalidBytes := []byte("invalid")
	_, err = internal.DecodeScalar[int](invalidBytes)
	assert.ErrorContains(t, err, "failed to unmarshal int")
}
