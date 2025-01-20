package rxn

import (
	"iter"
)

type MapState[K comparable, V any] struct {
	name     string
	original map[K]V
	updates  map[K]ValueUpdate[V]
	codec    MapStateCodec[K, V]
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

func NewMapState[K comparable, V any](name string, codec MapStateCodec[K, V]) *MapState[K, V] {
	return &MapState[K, V]{name, make(map[K]V), make(map[K]ValueUpdate[V]), codec}
}

func (s *MapState[K, V]) Set(key K, value V) {
	s.updates[key] = ValueUpdate[V]{
		Value: value,
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
	// Noop if neither map has the key
	if _, ok := s.original[key]; !ok {
		if _, ok := s.updates[key]; !ok {
			return
		}
	}

	s.updates[key] = ValueUpdate[V]{
		IsDelete: true,
	}
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

func (s *MapState[K, V]) Marshal() ([]byte, error) {
	return nil, nil
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
	return nil
}

func (s *MapState[K, V]) Name() string {
	return s.name
}

func (s *MapState[K, V]) Unmarshal(data []byte) error {
	panic("unimplemented")
}

var _ StateItem = (*MapState[any, any])(nil)
