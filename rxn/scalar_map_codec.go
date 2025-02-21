package rxn

import "reduction.dev/reduction-go/internal/states"

// ScalarMapCodec is a concrete [MapCodec] which uses Protobuf to
// serialize simple scalar values. Supported types are:
//   - int, int32, int64
//   - uint, uint32, uint64
//   - float32, float64
//   - string
//   - bool
//   - time.Time
type ScalarMapCodec[K comparable, V any] struct{}

// EncodeKey encodes the key using encodeScalar.
func (ScalarMapCodec[K, V]) EncodeKey(key K) ([]byte, error) {
	return states.EncodeScalar(any(key))
}

// DecodeKey decodes the key using decodeScalar.
func (ScalarMapCodec[K, V]) DecodeKey(b []byte) (K, error) {
	return states.DecodeScalar[K](b)
}

// Encode encodes the value using encodeScalar.
func (ScalarMapCodec[K, V]) EncodeValue(value V) ([]byte, error) {
	return states.EncodeScalar(value)
}

// Decode decodes the value using decodeScalar.
func (ScalarMapCodec[K, V]) DecodeValue(b []byte) (V, error) {
	return states.DecodeScalar[V](b)
}
