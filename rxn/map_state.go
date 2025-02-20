package rxn

import (
	"iter"

	"reduction.dev/reduction-go/internal/states"
)

// MapState provides access to key-value state
type MapState[K comparable, V any] struct {
	internal *states.MapState[K, V]
}

// Set updates the value for a key
func (s *MapState[K, V]) Set(key K, value V) {
	s.internal.Set(key, value)
}

// Get retrieves the value for a key
func (s *MapState[K, V]) Get(key K) (V, bool) {
	return s.internal.Get(key)
}

// Delete removes a key from the map
func (s *MapState[K, V]) Delete(key K) {
	s.internal.Delete(key)
}

// All returns an iterator over all entries in the map
func (s *MapState[K, V]) All() iter.Seq2[K, V] {
	return s.internal.All()
}

// Size returns the current number of items in the map
func (s *MapState[K, V]) Size() int {
	return s.internal.Size()
}

// NewMapState creates a new MapState instance with the given name and codec
func NewMapState[K comparable, V any](name string, codec MapCodec[K, V]) *MapState[K, V] {
	return &MapState[K, V]{
		internal: states.NewMapState(name, codec),
	}
}
