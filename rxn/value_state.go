package rxn

import (
	"reduction.dev/reduction-go/internal/states"
	"reduction.dev/reduction-go/topology"
)

// ValueState provides access to a single value of type T
type ValueState[T any] struct {
	internal *states.ValueState[T]
}

// Value returns the current value stored in the state
func (s *ValueState[T]) Value() T {
	return s.internal.Value()
}

// Set updates the value stored in the state
func (s *ValueState[T]) Set(value T) {
	s.internal.Set(value)
}

// Drop removes the value from the state
func (s *ValueState[T]) Drop() {
	s.internal.Drop()
}

// NewValueState creates a new ValueState instance with the given name and codec
func NewValueState[T any](name string, codec topology.ValueCodec[T]) *ValueState[T] {
	return &ValueState[T]{
		internal: states.NewValueState(name, codec),
	}
}
