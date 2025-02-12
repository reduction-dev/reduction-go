package rxn

import (
	"iter"

	"reduction.dev/reduction-go/internal"
)

type MapState[K comparable, V any] struct {
	name     string
	original map[K]V
	updates  map[K]ValueUpdate[V]
	codec    MapStateCodec[K, V]
	size     int // tracks current number of items
}

type ValueUpdate[V any] struct {
	IsDelete bool
	Value    V
}

type MapStateCodec[K comparable, V any] interface {
	EncodeKey(key K) ([]byte, error)
	DecodeKey(b []byte) (K, error)
	EncodeValue(value V) ([]byte, error)
	DecodeValue(b []byte) (V, error)
}

// MapStateOption is a function that applies a configuration to a MapState.
type MapStateOption[K comparable, V any] func(*MapState[K, V])

// WithCodec sets the codec for a MapState.
func WithCodec[K comparable, V any](codec MapStateCodec[K, V]) MapStateOption[K, V] {
	return func(s *MapState[K, V]) {
		s.codec = codec
	}
}

// NewMapState creates a new MapState, applying any provided options.
// If no codec option is provided, it defaults to using DefaultMapStateCodec.
func NewMapState[K comparable, V any](name string, opts ...MapStateOption[K, V]) *MapState[K, V] {
	ms := &MapState[K, V]{
		name:     name,
		original: make(map[K]V),
		updates:  make(map[K]ValueUpdate[V]),
		size:     0,
	}
	for _, opt := range opts {
		opt(ms)
	}
	if ms.codec == nil {
		// Use the default codec. Note: this works when K and V satisfy internal.ProtoScalar.
		ms.codec = ScalarMapStateCodec[K, V]{}
	}
	return ms
}

func (s *MapState[K, V]) Set(key K, value V) {
	_, hadKey := s.Get(key)

	s.updates[key] = ValueUpdate[V]{
		Value: value,
	}

	if !hadKey {
		s.size++
	}
}

func (s *MapState[K, V]) Get(key K) (V, bool) {
	if v, ok := s.updates[key]; ok {
		return v.Value, !v.IsDelete
	}
	v, ok := s.original[key]
	return v, ok
}

func (s *MapState[K, V]) Delete(key K) {
	_, hadKey := s.Get(key)
	if !hadKey {
		return
	}

	s.updates[key] = ValueUpdate[V]{
		IsDelete: true,
	}
	s.size--
}

func (s *MapState[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		// Go through all entries in original
		for k, v := range s.original {
			// If there is a value in updates, use that
			if uv, ok := s.updates[k]; ok {
				if uv.IsDelete {
					continue // Skip deleted items
				}
				if !yield(k, uv.Value) { // Using update value
					return
				}
				continue
			}
			if !yield(k, v) { // Using original value
				return
			}
		}

		// Go through all entries in updates
		for k, v := range s.updates {
			if _, ok := s.original[k]; ok {
				continue // If the key was in original it's all ready been yielded
			}
			if v.IsDelete {
				continue // Skip deleted entries
			}
			if !yield(k, v.Value) {
				return
			}
		}
	}
}

func (s *MapState[K, V]) Mutations() ([]StateMutation, error) {
	mutations := make([]StateMutation, 0, len(s.updates))
	for key, update := range s.updates {
		keyBytes, err := s.codec.EncodeKey(key)
		if err != nil {
			return nil, err
		}

		if update.IsDelete {
			mutations = append(mutations, &DeleteMutation{Key: keyBytes})
		} else {
			bs, err := s.codec.EncodeValue(update.Value)
			if err != nil {
				return nil, err
			}
			mutations = append(mutations, &PutMutation{Key: keyBytes, Value: bs})
		}
	}
	return mutations, nil
}

func (s *MapState[K, V]) Load(entries []StateEntry) error {
	result := make(map[K]V, len(entries))
	for _, e := range entries {
		key, err := s.codec.DecodeKey(e.Key)
		if err != nil {
			return err
		}
		value, err := s.codec.DecodeValue(e.Value)
		if err != nil {
			return err
		}
		result[key] = value
	}
	s.original = result
	s.size = len(result)
	return nil
}

func (s *MapState[K, V]) Name() string {
	return s.name
}

// Size returns the current number of items in the map state.
func (s *MapState[K, V]) Size() int {
	return s.size
}

var _ StateItem = (*MapState[any, any])(nil)

// ScalarMapStateCodec implements MapStateCodec for ProtoScalar types.
type ScalarMapStateCodec[K comparable, V any] struct{}

// EncodeKey encodes the key using encodeScalar.
func (ScalarMapStateCodec[K, V]) EncodeKey(key K) ([]byte, error) {
	return internal.EncodeScalar(any(key))
}

// DecodeKey decodes the key using decodeScalar.
func (ScalarMapStateCodec[K, V]) DecodeKey(b []byte) (K, error) {
	return internal.DecodeScalar[K](b)
}

// EncodeValue encodes the value using encodeScalar.
func (ScalarMapStateCodec[K, V]) EncodeValue(value V) ([]byte, error) {
	return internal.EncodeScalar(value)
}

// DecodeValue decodes the value using decodeScalar.
func (ScalarMapStateCodec[K, V]) DecodeValue(b []byte) (V, error) {
	return internal.DecodeScalar[V](b)
}
